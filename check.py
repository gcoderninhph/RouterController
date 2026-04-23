import os
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('await db.SortedSetAddAsync(""active_players_ts"", new SortedSetEntry[]', 
'await db.SortedSetAddAsync(""active_players_ts"", new SortedSetEntry[]')
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'w', encoding='utf8') as f:
    f.write(text)
