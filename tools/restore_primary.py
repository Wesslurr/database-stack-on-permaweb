import json
import re

def main():
    path = "/mnt/c/Users/Dovizioso/.gemini/antigravity-cli/brain/acdb0ddc-5d06-489e-b3c7-4b0998ace4f8/.system_generated/steps/3860/output.txt"
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    # Find "body": "...lua content..."
    idx = text.find('"body": "')
    if idx == -1:
        print("Not found!")
        return

    start = idx + len('"body": "')
    end = text.rfind('"\n')
    raw_segment = text[start:end]

    # json decode string
    decoded = json.loads('"' + raw_segment + '"')
    out_path = "/home/weslur/database/engine/processes/primary/primary_process.lua"
    with open(out_path, "w", encoding="utf-8") as out:
        out.write(decoded)
    print("SUCCESS: Restored", out_path, "bytes:", len(decoded))

if __name__ == "__main__":
    main()
