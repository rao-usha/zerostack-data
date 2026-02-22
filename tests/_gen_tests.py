# Generator script - writes test files
import os, base64, zlib
TESTS_DIR = r'C:\Users\awron\projects\Nexdata\tests'

# Content will be appended below by subsequent writes
def write_file(name, content):
    path = os.path.join(TESTS_DIR, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f'Written {path}')

# Read content from companion files
for name in ['test_export_service.py', 'test_webhook_service.py']:
    src = os.path.join(TESTS_DIR, f'_{name}.txt')
    if os.path.exists(src):
        with open(src, 'r', encoding='utf-8') as f:
            write_file(name, f.read())
        os.remove(src)
    else:
        print(f'Missing {src}')
