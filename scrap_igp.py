import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página del IGP'
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página del IGP'
        }

    headers = [th.text.strip() for th in table.find_all('th')]

    rows = []
    for tr in table.find_all('tr')[1:11]:  # solo 10 primeros
        tds = tr.find_all('td')
        if len(tds) == len(headers):
            row = {headers[i]: tds[i].text.strip() for i in range(len(tds))}
            rows.append(row)

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaSismosIGP')

    # Eliminar los datos anteriores
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'id': item['id']})

    # Insertar los nuevos datos
    for i, row in enumerate(rows, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
