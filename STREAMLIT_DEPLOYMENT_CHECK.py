from pathlib import Path
import sys
root = Path(__file__).resolve().parent
required = [
    root/'app.py', root/'settled_rules_engine.py',
    root/'PICK3_95.csv', root/'CORE45.csv', root/'MEMBER35.csv',
]
missing = [str(p.relative_to(root)) for p in required if not p.exists()]
if missing:
    print('FAIL: missing required deployment files:')
    print('\n'.join(f' - {x}' for x in missing))
    sys.exit(1)
print('PASS: V51.41 Streamlit deployment files are complete.')
