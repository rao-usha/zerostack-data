import base64  
s = 'hello world'  
e = base64.b64encode(s.encode()).decode()  
print(e) 
