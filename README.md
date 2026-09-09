# Patrón de sincronización e integración por eventos en Azure Data Factory

Implementación de referencia de una malla de integración que sincroniza un
sistema transaccional con un broker de mensajería, garantizando que ningún
registro se procese dos veces y que cualquier fallo sea recuperable de forma
automática.

Es una reconstrucción propia del patrón que diseñé e implementé en producción,
con dominio y nombres genéricos.

---

## El problema

Un sistema transaccional genera órdenes que deben publicarse en una cola para
que un microservicio downstream las consuma. Restricciones:

- El proceso corre en ciclos cortos y recurrentes.
- Una orden **nunca** debe publicarse dos veces (el consumidor no es idempotente).
- Si algo falla a mitad del proceso, la orden debe reintentarse sola.
- El fallo de un mensaje no debe arrastrar al resto del lote.
- Varios flujos comparten la misma infraestructura de staging.

---

## Arquitectura

```
        ┌──────────┐
        │ Trigger  │  cada N minutos
        └────┬─────┘
             ▼
   ┌─────────────────────┐
   │ sch_  Scheduler     │  aísla el flujo → activar/detener sin tocar otros
   └────┬────────────────┘
        ▼
   ┌─────────────────────┐
   │ pl_   Orquestador   │  ¿HAY trabajo? → sale limpio si no hay nada
   └────┬────────────────┘
        ▼
   ┌─────────────────────┐
   │ pip_  Procesador    │  reclama registros + proyecta a staging
   └────┬────────────────┘
        ▼
   ┌─────────────────────┐        ┌──────────┐        ┌───────────────┐
   │ exec_ Publicador    │ ─────► │  Broker  │ ─────► │ Microservicio │
   └─────────────────────┘        └──────────┘        └───────────────┘
```

Cada nivel tiene una única responsabilidad. Se pueden depurar de forma
independiente y se reutilizan entre flujos.

---

## Decisiones de diseño

### 1. Idempotencia por máquina de estados

El punto central del diseño. Cada registro lleva un campo de estado que **solo
el pipeline escribe**, separado del estado de negocio.

```
  PENDING ──► PROCESSING ──► DISPATCHED
     ▲             │
     └─────────────┘
        rollback
```

El filtro de entrada busca únicamente `PENDING`. Al reclamar los registros se
los marca como `PROCESSING` **antes** de trabajarlos, por lo que salen del
conjunto candidato: aunque una ejecución posterior arranque mientras la anterior
sigue corriendo, no puede tomar los mismos registros.

Esto da exclusión mutua sin locks, sin tablas de control y sin depender de la
configuración de concurrencia del orquestador.

### 2. Compensación explícita en cada punto de fallo

Cada transición de estado tiene su rama de reversión (`dependencyConditions: Failed`).
Si el fallo ocurre después de reclamar pero antes de publicar, el registro
vuelve a `PENDING` y el siguiente ciclo lo reintenta.

Sin compensación, un fallo deja registros huérfanos en el estado intermedio:
invisibles para el filtro de entrada y por tanto imposibles de reprocesar sin
intervención manual. Es el modo de fallo más costoso de este patrón.

La compensación del publicador es **por registro**, no por lote: si un mensaje
es rechazado, solo esa orden se revierte.

### 3. Separación entre estado de negocio y estado de sincronización

| Campo | Lo escribe | Rol |
|---|---|---|
| `BusinessStatus` | El proceso de negocio | Resultado de la orden |
| `SyncStatus` | **El pipeline** | Control de procesamiento |

El pipeline lee el estado de negocio para filtrar, pero nunca lo modifica.
Esta separación permite reprocesar sin riesgo de falsear un resultado de
negocio, y hace que el diagnóstico de incidentes sea inequívoco: si un registro
no avanza, el campo en el que está trabado indica de inmediato de quién es el
problema.

### 4. Staging compartido con particionamiento lógico

Varios flujos escriben en la misma tabla de staging. Cada fila lleva
`RunTimestamp` + `FlowType`, y todas las lecturas y borrados filtran por ambos.

Así se comparte infraestructura sin que los flujos interfieran entre sí, ni
siquiera cuando corren en paralelo. La limpieza final borra solo las filas de
su propia corrida.

### 5. Publicación secuencial

El `ForEach` del publicador es secuencial de forma deliberada. En producción,
un consumidor con capacidad limitada es el cuello de botella real: publicar en
paralelo solo adelanta la saturación.

> Aprendizaje de producción: cuando un componente aguas arriba se recupera tras
> una caída, el backlog acumulado se libera de golpe. Con ciclos más cortos que
> la duración de una ejecución, se superponen corridas y el volumen agregado
> puede desbordar al consumidor. La mitigación es limitar la concurrencia del
> orquestador y acotar el tamaño de lote por ciclo.

### 6. Configuración externalizada

Namespace del broker, nombre de cola e identificadores de aplicación son
parámetros, no literales. La URL se compone en tiempo de ejecución, lo que
permite promover el mismo artefacto entre ambientes cambiando solo los valores
del despliegue.

La autenticación es por identidad administrada: no hay credenciales en el
pipeline ni en el repositorio.

### 7. Contrato de integración explícito

El mensaje publicado separa metadatos de payload (`headers` / `data`), lleva un
identificador de correlación único por mensaje, y normaliza los tipos en origen:

- **Fechas** convertidas a texto ISO 8601 en la consulta, no en el pipeline.
  El consumidor no depende del formato regional del motor de base de datos.
- **Detalle anidado** serializado como array JSON con `FOR JSON PATH`, evitando
  tener que reconstruir la jerarquía en el orquestador.
- **Numéricos** publicados sin comillas, respetando el tipo acordado.

---

## Estructura

```
pipeline/
├── sch_orders_sync_scheduler.json     Malla — punto de entrada del trigger
├── pl_orders_sync_orchestrator.json   Orquestador — decide si hay trabajo
├── pip_orders_sync_processor.json     Procesador — reclama y proyecta
└── exec_orders_sync_dispatcher.json   Publicador — itera y publica
```

## Modelo de datos esperado

```sql
dbo.Orders            -- cabecera. Incluye BusinessStatus y SyncStatus
dbo.OrderItems        -- detalle
dbo.tmp_OrdersToJson  -- staging denormalizado, particionado por
                      -- RunTimestamp + FlowType
```

Los servicios vinculados (`ls_target_sqldb`) y los valores por ambiente se
resuelven en el despliegue.
