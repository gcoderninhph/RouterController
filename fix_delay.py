import os
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('await Task.Delay(10000, ct);', 'await Task.Delay(1000, ct);')
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'w', encoding='utf8') as f:
    f.write(text)
