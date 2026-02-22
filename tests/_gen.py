import os, sys
DIR = r'C:\Users\awron\projects\Nexdata\tests'
 
# Read b64 files and decode them 
import base64 
for fname in ['test_export_service', 'test_webhook_service']: 
    b64_path = os.path.join(DIR, fname + '.b64') 
    out_path = os.path.join(DIR, fname + '.py') 
    if os.path.exists(b64_path): 
        with open(b64_path, 'r') as f: 
            data = f.read().replace('\n', '').replace('\r', '').strip() 
        content = base64.b64decode(data).decode('utf-8') 
        with open(out_path, 'w', encoding='utf-8') as out: 
            out.write(content) 
        print(f'Written {len(content)} chars to {out_path}') 
    else: 
        print(f'Missing: {b64_path}')
