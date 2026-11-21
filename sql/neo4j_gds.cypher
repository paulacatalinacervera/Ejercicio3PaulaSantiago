// Proyeccion del grafo de zonas y relaciones TRIP agregadas.
CALL gds.graph.drop('zoneGraph', false);

CALL gds.graph.project(
  'zoneGraph',
  'Zone',
  {TRIP: {orientation: 'NATURAL', properties: ['trips', 'avg_duration_min']}}
);

// PageRank ponderado por cantidad de viajes.
CALL gds.pageRank.write('zoneGraph', {
  writeProperty: 'pagerank',
  relationshipWeightProperty: 'trips',
  maxIterations: 20,
  dampingFactor: 0.85
});

// Intermediacion ponderada por duracion promedio.
CALL gds.betweenness.write('zoneGraph', {
  writeProperty: 'betweenness',
  relationshipWeightProperty: 'avg_duration_min'
});

// Ruta mas corta por duracion promedio entre dos zonas.
MATCH (s:Zone {location_id: 161}), (t:Zone {location_id: 132})
CALL gds.shortestPath.dijkstra.stream('zoneGraph', {
  sourceNode: s,
  targetNode: t,
  relationshipWeightProperty: 'avg_duration_min'
})
YIELD path, totalCost
RETURN [n IN nodes(path) | n.zone] AS route, totalCost;
