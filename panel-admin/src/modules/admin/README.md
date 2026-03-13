# Admin Module Boundary

Este directorio define contratos y utilidades runtime específicos del panel admin.

Objetivo:

- Evitar dependencias implícitas entre admin y workspace.
- Centralizar configuración de hosts/orígenes para despliegue `admin.*` + `app.*`.
- Facilitar extracción a repositorio independiente.

Contenido:

- `contracts.ts`: tipos y contratos del proxy admin.
- `runtime.ts`: resolución de hosts/orígenes y enlaces admin->workspace.

