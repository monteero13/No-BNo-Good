# IDS-Refinery: Un Pipeline de Limpieza y Armonización de RDF

![Python](https://img.shields.io/badge/Python-3.7%2B-blue?logo=python&style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

Este repositorio contiene un pipeline ETL (Extract, Transform, Load) de Python diseñado para procesar archivos `catalogo.json` (JSON-LD) y convertirlos en un grafo de conocimiento limpio, estable y listo para producción.

El script está diseñado específicamente para resolver problemas comunes en los JSON-LD del mundo real y para **armonizar** un modelo DCAT estándar con el vocabulario de **International Data Spaces (IDS)**.

## 🚀 Características Principales

* **Skolemización de BNodes:** Elimina los identificadores anónimos (Nodos Blancos o BNodes) para `dcterms:creator` y `dcat:distribution`. Los reemplaza por URIs permanentes y deterministas (basadas en hashes y slugs) para que los datos sean estables y referenciables.

* **Limpieza Agresiva de Tipos:** Resuelve el problema común de las `owl:AnnotationProperty` incorrectas. El script **borra todas las definiciones de tipo** (`DatatypeProperty`, `ObjectProperty`, etc.) que provienen del contexto del JSON-LD original.

* **Detección y Re-tipado:** Vuelve a analizar el grafo limpio para **deducir el tipo correcto** de cada propiedad basándose en su uso real (si se usa con un literal o con una URI), reconstruyendo la ontología correctamente.

* **Procesamiento por Lotes (Store-and-Forward):** Utiliza un enfoque de "leer y escribir" para procesar el grafo en lotes de tamaño configurable (ej. 100,000 tripletas). Esto garantiza que pueda manejar archivos de cualquier tamaño sin agotar la RAM.

* **Armonización de Modelos:**
    * Mapea clases de DCAT a sus equivalentes de IDS (ej. `dcat:Dataset` -> `ids:Resource`).
    * Transforma propiedades clave de DCAT/ODRL a IDS (ej. `odrl:hasPolicy` -> `ids:hasContractOffer`).
    * Conserva y limpia vocabularios estándar (como `dcterms:title` o `dcterms:language`).

* **Exportación Rápida:** Genera un archivo `.nt` (N-Triples), un formato mucho más rápido y eficiente para cargar en bases de datos de grafos (Triplestores) que XML/RDF.

---

## 🔧 Cómo Funciona: El Pipeline

El script opera en una secuencia de pasos lógicos para garantizar un resultado limpio:

1.  **Carga y Skolemización:** Se carga el `catalogo.json`. Las funciones `skolemize_agents` y `skolemize_distributions` se ejecutan primero para resolver todos los BNodes a URIs estables.
2.  **Limpieza de Tipos:** Se eliminan *todas* las tripletas de tipado de propiedades (ej. `(dcterms:title, rdf:type, owl:AnnotationProperty)`) del grafo `g`.
3.  **Transformación (Batch):** Se crea un nuevo grafo `g_new`. El script itera sobre `g` en lotes, aplicando la lógica de transformación (mapeo de clases/propiedades IDS, limpieza de `mediaType`) y escribiendo el resultado limpio en `g_new`.
4.  **Detección y Re-tipado:** Una vez `g_new` está completo, el script lo analiza para *detectar* qué propiedades se usan con literales y cuáles con objetos. Luego, **reconstruye** los tipos correctos (ej. `(dcterms:title, rdf:type, owl:DatatypeProperty)`).
5.  **Axiomas y Exportación:** Finalmente, se añaden los axiomas (dominios y rangos) para las nuevas propiedades IDS y el grafo limpio se exporta como `catalog_ids_instances.nt`.

---

## 📦 Requisitos e Instalación (con `uv`)

Este proyecto usa `uv` y un archivo `pyproject.toml` para definir las dependencias.

Asegúrate de tener [uv instalado](https://github.com/astral-sh/uv) en tu sistema.

### 1. Crear el Entorno Virtual

En la raíz del repositorio, crea un nuevo entorno virtual:

```bash
uv venv

### 2. Activar el Entorno Virtual

Antes de instalar o ejecutar, debes activar el entorno:

**En macOS/Linux**

```bash
source .venv/bin/activate
```

**En Windows**

```bash
.venv\Scripts\activate
```

### 3. Instalar dependencias

```bash
uv pip install .
```

o

```bash
uv pip sync
```


