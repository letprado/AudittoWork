from flask import Flask, request, jsonify
import fitz  # PyMuPDF para ler o PDf
import base64
import requests  # Para enviar a requisição à API LLaMA
import json
import re

app = Flask(__name__)

# URL da API do LLaMA
LLAMA_API_URL = "http://dns.auditto.com.br:11434/api/generate"

# Primeira função: organiza o texto extraído em formato legível
def organizar_texto_llama(texto_extraido):
    prompt = f"""
Você é um assistente inteligente que organiza dados de notas fiscais eletrônicas (NFSe) em texto limpo e estruturado.

Sua tarefa é ler o conteúdo bruto extraído de um PDF de nota fiscal e organizar as informações de forma clara e legível.

INSTRUÇÕES:
1. Organize as informações em seções claras: TOMADOR, PRESTADOR, DADOS DA NOTA
2. Se algum campo não for encontrado, escreva "Não encontrado"
3. Nunca invente dados, use apenas o que está disponível no texto
4. Seja muito cuidadoso para não misturar dados do tomador com prestador
5. Apresente as informações de forma organizada e fácil de ler
6. Mantenha a formatação limpa

FORMATO DE RESPOSTA ESPERADO:

=== TOMADOR ===
Documento (CNPJ/CPF): [valor ou "Não encontrado"]
Inscrição Municipal: [valor ou "Não encontrado"]
Inscrição Estadual: [valor ou "Não encontrado"]
Nome: [valor ou "Não encontrado"]
Logradouro: [valor ou "Não encontrado"]
Número: [valor ou "Não encontrado"]
Bairro: [valor ou "Não encontrado"]
Complemento: [valor ou "Não encontrado"]
Cidade: [valor ou "Não encontrado"]
UF: [valor ou "Não encontrado"]
CEP: [valor ou "Não encontrado"]
País: [valor ou "Não encontrado"]
Telefone: [valor ou "Não encontrado"]
Email: [valor ou "Não encontrado"]
Código da Cidade: [valor ou "Não encontrado"]

=== PRESTADOR ===
Documento (CNPJ/CPF): [valor ou "Não encontrado"]
Inscrição Municipal: [valor ou "Não encontrado"]
Inscrição Estadual: [valor ou "Não encontrado"]
Nome: [valor ou "Não encontrado"]
Logradouro: [valor ou "Não encontrado"]
Número: [valor ou "Não encontrado"]
Bairro: [valor ou "Não encontrado"]
Complemento: [valor ou "Não encontrado"]
Cidade: [valor ou "Não encontrado"]
UF: [valor ou "Não encontrado"]
CEP: [valor ou "Não encontrado"]
País: [valor ou "Não encontrado"]
Telefone: [valor ou "Não encontrado"]
Email: [valor ou "Não encontrado"]
Código da Cidade: [valor ou "Não encontrado"]

=== DADOS DA NOTA ===
Número: [valor ou "Não encontrado"]
Chave de Acesso: [valor ou "Não encontrado"]
Data de Emissão: [valor ou "Não encontrado"]
Número RPS: [valor ou "Não encontrado"]
Série: [valor ou "Não encontrado"]
Código do Município: [valor ou "Não encontrado"]
Código do Serviço: [valor ou "Não encontrado"]
Descrição do Serviço: [valor ou "Não encontrado"]
Descrição da Nota: [valor ou "Não encontrado"]
Valor do Serviço: [valor ou "Não encontrado"]
Alíquota ISS: [valor ou "Não encontrado"]
Valor ISS: [valor ou "Não encontrado"]
PIS Retido: [valor ou "Não encontrado"]
COFINS Retido: [valor ou "Não encontrado"]
INSS Retido: [valor ou "Não encontrado"]
IRRF/IR Retido: [valor ou "Não encontrado"]
CSLL Retido: [valor ou "Não encontrado"]
Valor Total: [valor ou "Não encontrado"]

Conteúdo bruto da nota extraído do PDF:
\"\"\"
{texto_extraido}
\"\"\"
    """
    
    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    }
    
    try:
        response = requests.post(LLAMA_API_URL, json=payload, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print("Timeout ao conectar com a API LLaMA (Etapa 1)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição LLaMA (Etapa 1): {e}")
        return None
    
    if response.status_code == 200:
        return response.json().get("response", "").strip()
    return None

# Segunda função: converte o texto organizado em JSON estruturado
def converter_texto_para_json(texto_organizado):
    prompt = f"""
Você é um assistente que converte dados organizados de notas fiscais em JSON estruturado.

Sua tarefa é pegar o texto organizado abaixo e converter EXATAMENTE para o formato JSON especificado.

REGRAS IMPORTANTES:
1. Se um campo contém "Não encontrado"ou "-", use string vazia ""
2. Para valores monetários, extraia apenas os números (ex: "R$ 123,45" vira "123.45")
3. Para datas, use formato DD/MM/AAAA
4. Retorne APENAS o JSON válido, sem texto adicional
5. Use aspas duplas no JSON
6. Não invente dados que não estão no texto

FORMATO JSON EXATO:
{{
    "sucesso": true,
    "mensagem": "",
    "dados": {{
        "nota": {{
            "tomador": {{
                "documento": "",
                "im": "",
                "ie": "",
                "nome": "",
                "logradouro": "",
                "numero": "",
                "bairro": "",
                "complemento": "",
                "cidade": "",
                "uf": "",
                "cep": "",
                "pais": "",
                "telefone": "",
                "email": "",
                "cidadeCodigo": ""
            }},
            "prestador": {{
                "documento": "",
                "im": "",
                "ie": "",
                "nome": "",
                "logradouro": "",
                "numero": "",
                "bairro": "",
                "complemento": "",
                "cidade": "",
                "uf": "",
                "cep": "",
                "pais": "",
                "telefone": "",
                "email": "",
                "cidadeCodigo": ""
            }},
            "tipo": "inbox",
            "origem": "download",
            "numero": "",
            "chave": "",
            "emissaoData": "",
            "numeroRps": "",
            "serie": "",
            "municipioCodigo": "",
            "servicoCodigo": "",
            "descricaoServico": "",
            "descricao": "",
            "servicoValor": "",
            "issAliquota": "",
            "issValor": "",
            "pisRetido": "",
            "cofinsRetido": "",
            "inssRetido": "",
            "irrfRetido": "",
            "csllRetido": "",
            "deducaoValor": "",
            "totalValor": ""
        }},
        "arquivo": {{
            "nome": "",
            "tipo": "pdf"
        }}
    }}
}}

Texto organizado da nota fiscal:
\"\"\"
{texto_organizado}
\"\"\"

Retorne APENAS o JSON:
    """
    
    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    }
    
    try:
        response = requests.post(LLAMA_API_URL, json=payload, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print("Timeout ao conectar com a API LLaMA (Etapa 2)")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição LLaMA (Etapa 2): {e}")
        return None
    
    if response.status_code == 200:
        return response.json().get("response", "").strip()
    return None

def limpar_json_response(resposta_llama):
    """
    Limpa a resposta do LLaMA para extrair apenas o JSON válido
    """
    try:
        # Remove possíveis textos antes e depois do JSON
        resposta_llama = resposta_llama.strip()
        
        # Procura pelo início do JSON
        inicio_json = resposta_llama.find('{')
        fim_json = resposta_llama.rfind('}') + 1
        
        if inicio_json != -1 and fim_json != -1:
            json_str = resposta_llama[inicio_json:fim_json]
            
            # Tenta fazer o parse do JSON
            dados_json = json.loads(json_str)
            return dados_json
        else:
            return None
            
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado ao processar JSON: {e}")
        return None

def criar_json_erro(mensagem_erro):
    """
    Cria um JSON de erro no formato esperado
    """
    return {
        "sucesso": False,
        "mensagem": mensagem_erro,
        "dados": {
            "nota": {
                "tomador": {
                    "documento": "",
                    "im": "",
                    "ie": "",
                    "nome": "",
                    "logradouro": "",
                    "numero": "",
                    "bairro": "",
                    "complemento": "",
                    "cidade": "",
                    "uf": "",
                    "cep": "",
                    "pais": "",
                    "telefone": "",
                    "email": "",
                    "cidadeCodigo": ""
                },
                "prestador": {
                    "documento": "",
                    "im": "",
                    "ie": "",
                    "nome": "",
                    "logradouro": "",
                    "numero": "",
                    "bairro": "",
                    "complemento": "",
                    "cidade": "",
                    "uf": "",
                    "cep": "",
                    "pais": "",
                    "telefone": "",
                    "email": "",
                    "cidadeCodigo": ""
                },
                "tipo": "inbox",
                "origem": "download",
                "numero": "",
                "chave": "",
                "emissaoData": "",
                "numeroRps": "",
                "serie": "",
                "municipioCodigo": "",
                "servicoCodigo": "",
                "descricaoServico": "",
                "descricao": "",
                "servicoValor": "",
                "issAliquota": "",
                "issValor": "",
                "pisRetido": "",
                "cofinsRetido": "",
                "inssRetido": "",
                "irrfRetido": "",
                "csllRetido": "",
                "deducaoValor": "",
                "totalValor": ""
            },
            "arquivo": {
                "nome": "",
                "tipo": "pdf"
            }
        }
    }

@app.route('/generate', methods=['POST'])
def generate():
    if 'file' not in request.files:
        return jsonify(criar_json_erro('Arquivo PDF não enviado')), 400
    
    pdf_file = request.files['file']
    export_base64 = request.form.get('export_base64', 'false').lower() == 'true'
    
    # Extrai o texto do PDF
    try:
        pdf_bytes = pdf_file.read()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texto_extraido = ""
        
        for page in doc:
            texto_extraido += page.get_text()
        
        doc.close()
        
        if not texto_extraido.strip():
            return jsonify(criar_json_erro('PDF não contém texto extraível')), 400
            
    except Exception as e:
        return jsonify(criar_json_erro(f'Erro ao processar PDF: {str(e)}')), 500
    
    # ETAPA 1: Organiza o texto bruto em formato legível
    print("Etapa 1: Organizando texto extraído...")
    texto_organizado = organizar_texto_llama(texto_extraido)
    
    if not texto_organizado:
        return jsonify(criar_json_erro('Falha ao organizar texto com o LLaMA (Etapa 1)')), 500
    
    print("Texto organizado:")
    print(texto_organizado[:500] + "..." if len(texto_organizado) > 500 else texto_organizado)
    
    # ETAPA 2: Converte o texto organizado em JSON estruturado
    print("Etapa 2: Convertendo texto organizado para JSON...")
    resposta_llama = converter_texto_para_json(texto_organizado)
    
    if not resposta_llama:
        return jsonify(criar_json_erro('Falha ao converter texto para JSON com o LLaMA (Etapa 2)')), 500
    
    # Limpa e processa a resposta JSON
    dados_json = limpar_json_response(resposta_llama)
    
    if not dados_json:
        return jsonify(criar_json_erro('Resposta do LLaMA não é um JSON válido')), 500
    
    # Adiciona o nome do arquivo
    if 'dados' in dados_json and 'arquivo' in dados_json['dados']:
        dados_json['dados']['arquivo']['nome'] = pdf_file.filename or 'documento.pdf'
    
    # Se solicitado, adiciona o base64 do PDF
    if export_base64:
        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
        dados_json['pdf_base64'] = pdf_base64
    
    return jsonify(dados_json)

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5050))
    app.run(debug=False, port=port, host='0.0.0.0')
