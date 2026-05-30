import requests
import os


def main(event):
    token = os.environ.get("Whisper_LAI")
    if not token:
        return {"outputFields": {"transcricao": "erro", "status": "erro", "debug": "secret Whisper_LAI nao carregado"}}

    jobId = event.get("inputFields", {}).get("jobId")
    if not jobId:
        return {"outputFields": {"transcricao": "erro", "status": "erro", "debug": "jobId nao mapeado no input da etapa 4"}}

    url_api = f"https://transcription.lai.ia.br/v1/audio/transcriptions/jobs/{jobId}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url_api, headers=headers, timeout=30)
        if response.status_code != 200:
            return {"outputFields": {
                "transcricao": "erro",
                "status": "erro",
                "debug": f"HTTP {response.status_code}: {response.text[:300]}",
            }}

        data = response.json()
        status = data.get("status")
        texto = (data.get("result") or {}).get("text", "")

        return {"outputFields": {
            "transcricao": texto if texto else "",
            "status": status,
            "debug": "",
        }}

    except Exception as e:
        return {"outputFields": {"transcricao": "erro", "status": "erro", "debug": f"excecao: {e}"}}
