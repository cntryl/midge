from pathlib import Path
p=Path('d:/repos/cntryl/midge/src/metadata/journal.rs')
s=p.read_text(encoding='utf8')
old='\n            .expect("MANIFEST_SYNC_STATE poisoned");'
if old in s:
    s=s.replace(old,'\n            .lock();')
    p.write_text(s,encoding='utf8')
    print('replaced')
else:
    print('pattern not found')