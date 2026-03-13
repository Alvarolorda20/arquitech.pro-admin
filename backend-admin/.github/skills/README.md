# Skills de Copilot (repositorio)

Este directorio contiene skills compartidas para que GitHub Copilot las use en este proyecto.

## Estructura

- `frontend-design/`
  - `SKILL.md`: instrucciones de la skill.
  - `LICENSE.txt`: licencia original.

## Uso

- Copilot detecta skills ubicadas en `.github/skills/<nombre-skill>/SKILL.md`.
- Al abrir este repositorio en VS Code con Copilot habilitado, la skill queda disponible para este proyecto.

## Mantenimiento

- Para actualizar una skill:
  1. Reinstalala o descargala desde su origen.
  2. Reemplaza la carpeta correspondiente dentro de `.github/skills/`.
  3. Revisa cambios en `SKILL.md` y haz commit.

- Si agregas otra skill, crea una nueva carpeta en `.github/skills/<nombre>/` y asegurate de incluir `SKILL.md`.
