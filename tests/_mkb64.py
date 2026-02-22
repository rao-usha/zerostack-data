import base64
content = open('C:/Users/awron/projects/Nexdata/tests/test_export_service.py', 'rb').read() 
enc = base64.b64encode(content).decode() 
print(len(enc))
