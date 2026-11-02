import json

def main():
    path = "/mnt/c/Users/Dovizioso/.gemini/antigravity-cli/brain/acdb0ddc-5d06-489e-b3c7-4b0998ace4f8/.system_generated/logs/transcript_full.jsonl"
    with open(path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f, 1):
            if idx == 1897:
                obj = json.loads(line)
                for tc in obj.get("tool_calls", []):
                    if tc.get("name") == "write_file" and "index.html" in tc.get("args", {}).get("Path", ""):
                        content = tc["args"]["Content"]
                        
                        # Apply refinements from steps 3027, 3029, 3031, 3035:
                        # 1. Batch Every 100 Ticks
                        content = content.replace(
                            '<span class="tier-tag crypto">Batch Every 5 Ticks</span>',
                            '<span class="tier-tag crypto" id="tier3BatchBadge">Batch Every 100 Ticks</span>'
                        )
                        # 2. NO_CACHE_HEADERS
                        content = content.replace(
                            'const NO_CACHE_HEADERS = {\n      "Cache-Control": "no-cache, no-store",\n      "Pragma": "no-cache"\n    };',
                            'const NO_CACHE_HEADERS = {\n      "Cache-Control": "no-cache"\n    };'
                        )
                        # 3. Batch 100 tick text
                        content = content.replace(
                            'const bInt = arbitState.batch_interval ?? 5;',
                            'const bInt = arbitState.batch_interval ?? 100;\n        const batchBadge = document.getElementById("tier3BatchBadge");\n        if (batchBadge) batchBadge.textContent = `Batch Every ${bInt} Ticks`;'
                        )
                        
                        out_path = "/home/weslur/database/tools/game_client/index.html"
                        with open(out_path, "w", encoding="utf-8") as out:
                            out.write(content)
                        print("SUCCESS: Restored", out_path, "bytes:", len(content))
                        return
    print("Step 1897 not found!")

if __name__ == "__main__":
    main()
