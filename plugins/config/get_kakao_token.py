import requests
#  client_id, authorized_code 노출금지, 실제 값은 임시로만 넣고 git에 올라가지 않도록 주의  #

client_id = '557319df29e510dfd5be88bfc78c3eb3'
redirect_uri = 'https://example.com/oauth'
authorize_code = 'm9oYNlwvJ0jSEKDtucoUTBLnD-UHOaWe-WAVJjVtPqVUYf33OR9saAAAAAQKPXMXAAABkFNt1cTOkqTnJF629A'


token_url = 'https://kauth.kakao.com/oauth/token'
data = {
    'grant_type'   : 'authorization_code' ,
    'client_id'    : client_id ,
    'redirect_uri' : redirect_uri ,
    'code'         : authorize_code
}

response = requests.post(token_url, data=data)
tokens = response.json()
print(tokens)