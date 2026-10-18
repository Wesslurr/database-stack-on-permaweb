local json = {
    decode = function(str)
        if not str or str == "" or str == "{}" then return {} end
        -- Basic number extraction for new_epoch
        local num = str:match('"new_epoch"%s*:%s*(%d+)') or str:match('"epoch"%s*:%s*(%d+)')
        local node = str:match('"node"%s*:%s*"([^"]+)"')
        return { new_epoch = num and tonumber(num), node = node }
    end,
    encode = function(tbl)
        return "{}"
    end
}
package.loaded["json"] = json

local arb = require("ao_process.arbitrator")

-- Test 1: PromoteEpoch by untrusted caller
local res1 = arb.handle({
    From = "ATTACKER_NODE",
    Tags = { { name = "Action", value = "PromoteEpoch" } },
    Data = "{}"
})

print("TEST1_HANDLED=" .. tostring(res1.handled))
print("TEST1_STATUS=" .. tostring(res1.responses[1] and res1.responses[1].Status))

-- Test 2: PromoteEpoch by authorized admin
local res2 = arb.handle({
    From = "ADMIN_PRIMARY",
    Tags = { { name = "Action", value = "PromoteEpoch" } },
    Data = '{"new_epoch":2}'
})

print("TEST2_HANDLED=" .. tostring(res2.handled))
print("TEST2_STATUS=" .. tostring(res2.responses[1] and res2.responses[1].Status))
print("TEST2_NEW_EPOCH=" .. tostring(res2.current_epoch))

-- Test 3: AuthorizeNode & RevokeNode
local res3 = arb.handle({
    From = "OWNER_PRIMARY",
    Tags = { { name = "Action", value = "AuthorizeNode" } },
    Data = '{"node":"SU_NEW_1"}'
})
print("TEST3_STATUS=" .. tostring(res3.responses[1] and res3.responses[1].Status))
