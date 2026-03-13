def get_pauta_segment(full_pauta, task_capitulos):
    """
    Busca correspondencias entre los capítulos del planificador 
    y los capítulos de la pauta mapeada.
    """
    segment = []
    for cap_pauta in full_pauta:
        pauta_name = cap_pauta.get('capitulo_nombre', "").lower()
        
        for cap_task in task_capitulos:
            task_name = cap_task.lower()
            
            # Buscamos coincidencia parcial (ej: "ESTRUCTURES" en "CAPITOL 04 ESTRUCTURES")
            if pauta_name in task_name or task_name in pauta_name:
                segment.append(cap_pauta)
                break
    return segment