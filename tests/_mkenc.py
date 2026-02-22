import base64
ps_script = 'Write-Host hello-from-ps'
encoded = base64.b64encode(ps_script.encode('utf-16-le')).decode()
print(encoded)
