import json

def main():
    path = "/mnt/c/Users/Dovizioso/.gemini/antigravity-cli/brain/acdb0ddc-5d06-489e-b3c7-4b0998ace4f8/.system_generated/logs/transcript_full.jsonl"
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, 1):
            if idx == 1841:
                obj = json.loads(line)
                for tc in obj.get("tool_calls", []):
                    if tc.get("name") == "write_file" and "test_e2e_2d_points.py" in tc.get("args", {}).get("Path", ""):
                        content = tc["args"]["Content"]
                        out_path = "/home/weslur/database/tests/test_e2e_2d_points.py"
                        with open(out_path, "w", encoding="utf-8") as out:
                            out.write(content)
                        print("SUCCESS: Restored", out_path, "bytes:", len(content))
                        return
    print("Line 1841 not matched or not found!")

if __name__ == "__main__":
    main()
