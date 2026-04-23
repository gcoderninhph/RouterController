import os
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('""active_players""', '\"active_players\"')
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'w', encoding='utf8') as f:
    f.write(text)
