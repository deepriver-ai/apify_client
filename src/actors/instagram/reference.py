'''
Reference file for some Instagram actors. Not supposed to be run or used.
'''


def scrape_instagram_urls(direct_urls, resultsType='posts', resultsLimit=200, filename=None, posts_newer_than=None):
    """
    Instagram Posts Scraper - scrapes posts from specific profile URLs or post URLs
    Actor: https://console.apify.com/actors/shu8hvrXbJbY3Eb9W/
    
    Args:
        direct_urls: List of Instagram profile URLs or post URLs to scrape
        resultsType: Type of results to scrape ('posts', 'reels', 'tagged', etc.)
        resultsLimit: Maximum number of results per URL
        filename: Optional filename to save results to (will save to data/ig/{filename})
    
    Returns:
        List of post items
    """
    run_input = {
        "directUrls": direct_urls,
        "resultsType": resultsType,
        "resultsLimit": resultsLimit,
        "onlyPostsNewerThan": posts_newer_than,
        #"search": None,
        #"searchType": "hashtag",
        #"searchLimit": 1,
        #"addParentData": False,
    }

    # Run the Actor and wait for it to finish
    run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

    # Fetch and print Actor results from the run's dataset (if there are any)
    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    '''
    sample post:
      {
        "inputUrl": "https://www.instagram.com/p/DSfb7XUD-G8/",
        "id": "3791872238006821308",
        "type": "Image",
        "shortCode": "DSfb7XUD-G8",
        "caption": "Planes personalizados, Cotízalo y contrata hoy!\n\nCobertura Nacional\n\nTel. 449 276 9206 y WhatsApp 449 195 8293👇\n\nPide tu cotización y asesoría\n#seguroshi #hiagentedeseguros #seguroauto #qualitas #mapfre #axa #anaseguros",
        "hashtags": [
          "seguroshi",
          "hiagentedeseguros",
          "seguroauto"
        ],
        "mentions": [],
        "url": "https://www.instagram.com/p/DSfb7XUD-G8/",
        "commentsCount": 0,
        "firstComment": "",
        "latestComments": [],
        "dimensionsHeight": 1080,
        "dimensionsWidth": 1080,
        "displayUrl": "https://instagram.ftpa1-2.fna.fbcdn.net/v/t39.30808-6/597379899_1426784282786274_2732495271432963601_n.jpg?stp=dst-jpg_e15_fr_s1080x1080_tt6&_nc_ht=instagram.ftpa1-2.fna.fbcdn.net&_nc_cat=106&_nc_oc=Q6cZ2QH4_r4hlaW7id2_vQejLulnvkyM0Lq4IHBufNgp4ohaVVNbI4bZl6eiXic7R5HMpR-CViugzxkQk8LMo3KeXoOu&_nc_ohc=PyCJ98TpjeAQ7kNvwGPCNPy&_nc_gid=uh7l6AHhPp9FFBevHNBZOg&edm=APs17CUAAAAA&ccb=7-5&oh=00_Afn7z2UXvxVfL22YG4K-DYcfsWktJ-ugMOqzSERCCG8xFA&oe=694D3E2C&_nc_sid=10d13b",
        "images": [],
        "alt": "Photo by Hugo Isaias | Seguros - Ahorro - Retiro. on December 20, 2025. May be an image of water heater and text that says 'MAPFRE SU FUTURO MERECE ESTAR PROTEGIDO'.",
        "likesCount": 0,
        "timestamp": "2025-12-20T16:00:17.000Z",
        "childPosts": [],
        "ownerFullName": "Hugo Isaias | Seguros - Ahorro - Retiro.",
        "ownerUsername": "hiagentedeseguros",
        "ownerId": "735691399",
        "isCommentsDisabled": false
      }
    '''
    
    if filename:
        filepath = f'data/ig/{filename}'
        with open(filepath, 'w') as f:
            json.dump(results, f)
    
    return results

def get_instagram_transcript(instagram_url=None, bulk_urls=None, word_level_timestamps=False, fast_processing=False):
    """
    Instagram AI Transcript Extractor
    Actor: https://console.apify.com/actors/3C7L8IMQOkq3isV2Y/

    Args:
        instagram_url: Single Instagram URL (mutually exclusive with bulk_urls)
        bulk_urls: List of Instagram URLs for bulk processing (mutually exclusive with instagram_url)
        word_level_timestamps: Whether to include word-level timestamps
        fast_processing: Whether to use fast processing mode
    
    Returns:
        List of transcript items. Each item contains an 'instagramUrl' field.
    """
    
    # Validate that exactly one parameter is provided
    if instagram_url is None and bulk_urls is None:
        raise ValueError("Either instagram_url or bulk_urls must be provided")
    if instagram_url is not None and bulk_urls is not None:
        raise ValueError("Only one of instagram_url or bulk_urls can be provided, not both")

    run_input = {
        "wordLevelTimestamps": word_level_timestamps,
        "fastProcessing": fast_processing,
    }

    if instagram_url is not None:
        run_input['instagramUrl'] = instagram_url
    if bulk_urls is not None:
        run_input['bulkUrls'] = bulk_urls

    ig_transcript_run = client.actor("3C7L8IMQOkq3isV2Y").call(run_input=run_input)

    items = []
    for item in client.dataset(ig_transcript_run["defaultDatasetId"]).iterate_items():
        items.append(item)

    '''
        sample item:
        {
        'transcript': 'Si te impactó la noticia de AXA y Grupo Ángeles tienes que escuchar.',
        'segments': [{'text': 'Si te impactó la noticia de AXA y Grupo Ángeles tienes que escuchar esto porque el problema va mucho más profundo.',
            'start': 0.46,
            'end': 6.88},
        {'text': 'En los últimos días se ha hablado mucho del retiro del convenio entre AXA y la cadena de hospitales del Grupo Ángeles,',
            'start': 7.34,
            'end': 13.4},
        {'text': 'pero muy pocos están explicando por qué estamos llegando a este punto.',
            'start': 13.74,
            'end': 18.24},
        {'text': 'La realidad es que desde hace años en algunos hospitales se vienen detectando prácticas que inflan las cuentas de manera importante.',
            'start': 18.8,
            'end': 26.84},
        {'text': 'como costos hospitalarios por muy encima del mercado, estudios duplicados, medicamentos no homologados,',
            'start': 27.26,
            'end': 35.08},...
            ],
        'words': [{'word': 'Si', 'start': 0.46, 'end': 0.5},
        {'word': 'te', 'start': 0.5, 'end': 0.64},
        {'word': 'impactó', 'start': 0.64, 'end': 0.98},
        {'word': 'la', 'start': 0.98, 'end': 1.14},
        {'word': 'noticia', 'start': 1.14, 'end': 1.64},
        {'word': 'de', 'start': 1.64, 'end': 1.82},
        {'word': 'AXA', 'start': 1.82, 'end': 2.12},
        {'word': 'y', 'start': 2.12, 'end': 2.2},
        {'word': 'Grupo', 'start': 2.2, 'end': 2.42},...
        ],
        'displayUrl': 'https://scontent-ord5-3.cdninstagram.com/v/...',
        'instagramUrl': 'https://www.instagram.com/p/DR79YJUj_nE/',
        'shortCode': 'DR79YJUj_nE',
        'caption': 'Lo que está pasando entre AXA y Grupo Ángeles tiene un trasfondo que nadie está explicando: ...',
        'hashtags': 'GastosMédicosMayores, AXA, GrupoÁngeles, SegurosMéxico, ...',
        'ownerUsername': 'adyllerenas',
        'ownerFullName': 'Adriana Llerenas',
        'videoDuration': 138.633,
        'timestamp': '2025-12-06T21:21:15.000Z',
        'videoUrl': 'https://scontent-ord5-3.cdninstagram.com/o1/v/t16/f2/...',
        'likesCount': 5412,
        'videoViewCount': 78125,
        'videoPlayCount': 158494,
        'commentsCount': 532,
        'musicArtist': 'adyllerenas',
        'musicSong': 'Original audio',
        'usesOriginalAudio': True,
        'locationName': '',
        'mentions': '',
        'firstComment': 'No tendria mayor problema si tuviéramos una COFECE ...',
        'id': '3781886252220938692',
        'type': 'Video',
        'url': 'https://www.instagram.com/p/DR79YJUj_nE/',
        'ownerId': '239826773',
        'locationId': '',
        'productType': 'clips',
        'isSponsored': False,
        'audioId': '25623976687234613',
        'isCommentsDisabled': False,
        'dimensionsHeight': 1136,
        'dimensionsWidth': 640,
        'processedAt': '2025-12-22T18:37:56.305Z',
        'status': 'success',
        'metadata': {'runId': 'AV0bqXj2vdaJ0JdI0',
        'actorId': '3C7L8IMQOkq3isV2Y',
        'processingType': 'single-processing',
        'urlIndex': 1,
        'totalUrls': 1,
        'userTier': 'PAID',
        'processingSpeed': 'standard'}}
    '''
    return items

def get_instagram_comments(direct_urls, resultsLimit=15):
    '''
    Obtiene comentarios para una lista de URLs de Instagram usando la API de Apify.
    
    Args:
        direct_urls (list): Lista de URLs de los posts de Instagram.
        resultsLimit (int): Número máximo de comentarios por post.
    
    Returns:
        list: Lista de comentarios con metadatos del post.
    '''
    run_input = {
        "directUrls": direct_urls,
        "includeNestedComments": False,
        "isNewestComments": False,
        "resultsLimit": resultsLimit  
    }

    # Run the Actor and wait for it to finish
    run = client.actor("SbK00X0JYCPblD2wp").call(run_input=run_input)

    # Fetch and print Actor results from the run's dataset (if there are any)
    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    print('\n\n-------------------------------- dataset id:, ', run["defaultDatasetId"], '\n\n')
    '''
    sample item:
    {
        "postUrl": "https://www.instagram.com/p/DR5OKYbkiha/",
        "commentUrl": "https://www.instagram.com/p/DR5OKYbkiha/c/18152724082420831",
        "id": "18152724082420831",
        "text": "Infórmate bien o no sabes leer.\nSolo es para su producto Plus.\nSabe cuántos asegurados de AXA tienen...",
        "ownerUsername": "barushmariocervantes",
        "ownerProfilePicUrl": "https://scontent-sjc6-1.cdninstagram.com/v/t51.7576...",
        "timestamp": "2025-12-07T23:29:10.000Z",
        "repliesCount": 4,
        "replies": [],
        "likesCount": 11,
        "owner": {
        "fbid_v2": "17841409391783762",
        "full_name": "",
        "id": "9470097059",
        "is_mentionable": true,
        "is_private": true,
        "is_verified": false,
        "profile_pic_id": "3630578946266510104",
        "profile_pic_url": "https://scontent-sjc6-1.cdninstagram.com/v/t51.7576...",
        "username": "barushmariocervantes"
    }
    '''

    return results
