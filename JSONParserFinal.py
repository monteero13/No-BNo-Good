

import re
import hashlib
from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal, URIRef, BNode
from rdflib.namespace import DCAT, DCTERMS, FOAF, XSD
import time
import itertools

# --- Namespaces ---
IDS   = Namespace("https://w3id.org/idsa/core/")
EDC   = Namespace("https://w3id.org/edc/v0.0.1/ns/")
ODRL  = Namespace("http://www.w3.org/ns/odrl/2/")
BASE  = "https://khaos.example.org"

BATCH_SIZE = 100000 

CLASS_MAP = {
    DCAT.Catalog:      IDS.Catalog,
    DCAT.Dataset:      IDS.Resource,
    DCAT.Distribution: IDS.Representation,
    DCAT.DataService:  IDS.Artifact
}

OBJ_PROP_MAP = {
    DCAT.dataset:      IDS.hasResource,
    DCAT.distribution: IDS.hasRepresentation,
    ODRL.hasPolicy:    IDS.hasContractOffer,
    DCTERMS.creator:   IDS.hasCreator
}

DATA_PROP_MAP ={} 

start_time = time.time()
g = Graph()
for prefix, ns in [
    ("ids", IDS), ("edc", EDC), ("dcat", DCAT),
    ("dct", DCTERMS), ("odrl", ODRL),
    ("foaf", FOAF), ("owl", OWL), ("rdfs", RDFS), ("xsd", XSD)
]:
    g.bind(prefix, ns)

g.parse("catalogo.json", format="json-ld", publicID=f"{BASE}/catalogo/")

def _slugify(text: str) -> str:
    """ Genera un 'slug' limpio para usar en una URI a partir de un texto. 
    ej. Jöan Pérez (Técnico) ---> joan-perez-tecnico """
    text = re.sub(r"\s+", "-", text.strip().lower()) #Eliminamos espacios  
    text = re.sub(r"[^a-z0-9\-]", "", text) #Eliminamos carácteres raros 
    return re.sub(r"-{2,}", "-", text).strip("-") or "agent" #Eliminamos guiones dobles o finales 

def skolemize_agents(graph: Graph, base_uri: str) -> Graph:
    """
    Convierte BNodes de creadores (dct:creator) en URIs estables.
    """
    creator_map = {}
    
   # Encontramos todas las tripletas (sujeto, dcterms:creator, nodo_creador) 
   # Si es un BNode se busca su (foaf:name), creamos un slug, un hash y 
   # Contruimos una URI permanente. 

    for s, _, node in graph.triples((None, DCTERMS.creator, None)):
        if isinstance(node, BNode) and node not in creator_map:
            name = graph.value(subject=node, predicate=FOAF.name)
            if not name: continue
            
            name_str = str(name).strip()
            slug = _slugify(name_str)
            h = hashlib.sha1(name_str.encode("utf-8")).hexdigest()[:8]
            new_uri = URIRef(f"{base_uri}/agent/{slug}-{h}")
            creator_map[node] = new_uri

   # Movemos las propiedades del BNode a la nueva URI
   # Reemplazamos referencias
   # Añadimos tipo
   # Limpiamos los BNodes

    for old_bnode, new_uri in creator_map.items():
        for p, o in graph.predicate_objects(old_bnode):
            graph.add((new_uri, p, o))
        for s in graph.subjects(DCTERMS.creator, old_bnode):
            graph.remove((s, DCTERMS.creator, old_bnode))
            graph.add((s, DCTERMS.creator, new_uri))
        graph.add((new_uri, RDF.type, FOAF.Agent))
        graph.remove((old_bnode, None, None))
    
    return graph

def skolemize_distributions(graph: Graph) -> Graph:
    """
    Convierte BNodes de distribuciones (dcat:distribution) en URIs estables.
    """

    # Encuentra todos los datasets
    # Para cada dataset miramos sus dcat.distribution
    # Si es una distributiom, creamos una URI permanente
      
    changes_to_make = []
    for ds in graph.subjects(RDF.type, DCAT.Dataset): 
        for dist_node in graph.objects(ds, DCAT.distribution): 
            if isinstance(dist_node, BNode):
                h = hashlib.sha1(str(dist_node).encode()).hexdigest()
                new_uri = URIRef(f"{ds}/representation/{h}")
                changes_to_make.append((ds, dist_node, new_uri))


    # Movemos propiedades
    # Eliminamos BNode antigui
    # Reemplazamos referencias
    # Añadimos el tipo
     
    for ds, dist_node, new_uri in changes_to_make:
        for p, o in graph.predicate_objects(dist_node): 
            graph.add((new_uri, p, o))
        graph.remove((None, None, dist_node)) 
        graph.remove((ds, DCAT.distribution, dist_node))
        graph.add((ds, DCAT.distribution, new_uri))
        graph.add((new_uri, RDF.type, DCAT.Distribution))
                
    return graph

g = skolemize_agents(g, BASE)
g = skolemize_distributions(g)

# Lista de todos los tipos de propiedad que pueden venir del @context
property_types_to_remove = [
    OWL.AnnotationProperty,
    OWL.DatatypeProperty,
    OWL.ObjectProperty,
    RDF.Property 
]

# Recopilar todas las tripletas de tipado de propiedades
triples_to_remove = []
for prop_type in property_types_to_remove:
    # Encontrar (cualquier_propiedad, rdf:type, tipo_a_borrar)
    for s, p, o in g.triples((None, RDF.type, prop_type)):
        # Asegurarse de que el sujeto es una URI (una propiedad) y no un BNode
        if isinstance(s, URIRef): 
            triples_to_remove.append((s, p, o))

for triple in triples_to_remove:
    g.remove(triple)

def process_chunk(chunk, target_graph):
    """
    Procesa un lote de tripletas y añade los resultados
    al grafo de destino (target_graph).
    """
    
    # Usamos una lista temporal para almacenar las tripletas
    # que se añadirán al grafo de destino.
    triples_to_add_to_target = []

    for s, p, o in chunk:
        
        # Eliminamps literales vacíos ---
        if isinstance(o, Literal) and not str(o).strip():
            continue # Simplemente no lo añadimos al nuevo grafo
    
        elif p == RDF.type and o in CLASS_MAP:
            triples_to_add_to_target.append((s, p, o)) 
            triples_to_add_to_target.append((s, RDF.type, CLASS_MAP[o]))
            continue

        elif p in OBJ_PROP_MAP:
            triples_to_add_to_target.append((s, OBJ_PROP_MAP[p], o))
            continue 

        elif p == DCAT.mediaType and isinstance(o, Literal):
            media_str = str(o).strip().lower()
            if "/" in media_str:
                uri = URIRef(f"http://www.iana.org/assignments/media-types/{media_str}")
            else:
                uri = URIRef("http://www.iana.org/assignments/media-types/text/plain")
            # Añadimos la tripleta con la URI transformada
            triples_to_add_to_target.append((s, DCAT.mediaType, uri))
            continue # El original (con el literal) no se añade
        
        else:
            triples_to_add_to_target.append((s, p, o))

    # Aplicamos el lote procesado al grafo de destino ---
    for triple in triples_to_add_to_target:
        target_graph.add(triple)
    
    return len(triples_to_add_to_target)


# Creamos el nuevo grafo de destino ---
g_new = Graph()
for prefix, ns in g.namespaces():
    g_new.bind(prefix, ns)

# Obtenemos el iterador del grafo original
triplet_iterator = g.__iter__()
total_processed = 0

while True:
    # Leer un lote de BATCH_SIZE del grafo original
    # 'list()' fuerza la evaluación del generador 'islice'
    chunk = list(itertools.islice(triplet_iterator, BATCH_SIZE))
    
    if not chunk:
        break # No hay más tripletas, terminamos el bucle

    # Procesamos el lote y lo escribimos en g_new
    count_added = process_chunk(chunk, g_new)    
    total_processed += len(chunk)

g = g_new

# Ahora rellenamos los sets DESPUÉS de que los tipos incorrectos hayan sido eliminados
literal_props_seen = set(p for s, p, o in g if isinstance(o, Literal))
object_props_seen = set(p for s, p, o in g if isinstance(o, (URIRef, BNode)))

# 1. Corregir propiedades de datos (Literales)
# Esto AHORA corregirá dcterms:license, dcterms:spatial, etc.,
# porque sus tipos incorrectos fueron borrados.
for prop in literal_props_seen:
    g.add((prop, RDF.type, OWL.DatatypeProperty))

# 2. Corregir propiedades de objeto (URIs/BNodes)
object_props_seen.discard(RDF.type) # rdf:type es un caso especial

for prop in object_props_seen:
    g.add((prop, RDF.type, OWL.ObjectProperty))

# --- Definir propiedades de objeto IDS (dominios y rangos) ---
domains = {
    IDS.hasResource:       IDS.Catalog,
    IDS.hasRepresentation: IDS.Resource,
    IDS.hasContractOffer:  IDS.Resource,
    IDS.hasCreator:        IDS.Resource
}
ranges = {
    IDS.hasResource:       IDS.Resource,
    IDS.hasRepresentation: IDS.Representation,
    IDS.hasContractOffer:  IDS.ContractOffer,
    IDS.hasCreator:        FOAF.Agent
}
for prop in OBJ_PROP_MAP.values():
    if prop in domains:
        g.add((prop, RDFS.domain, domains[prop]))
        g.add((prop, RDFS.range, ranges[prop]))

# --- Definir propiedades de datos IDS (dominios y rangos) ---
for prop in DATA_PROP_MAP.values():
    for cls in CLASS_MAP.values():
        g.add((prop, RDFS.domain, cls))
    g.add((prop, RDFS.range, RDFS.Literal))

# --- Definir axiomas para dcat:mediaType ---
g.add((DCAT.mediaType, RDFS.domain, DCAT.Distribution))
g.add((DCAT.mediaType, RDFS.range, URIRef("http://purl.org/dc/terms/MediaTypeOrExtent")))

outfile = "catalog_ids_instances.nt" # Formato N-Triples
g.serialize(destination=outfile, format="ntriples", encoding="utf-8") 

print(f"Exportación completada en {time.time() - start_time:.2f}s. Total tripletas: {len(g)}")