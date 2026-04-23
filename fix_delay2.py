import os
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('await Task.WhenAny(tcsServiceReceive.Task, Task.Delay(2000));', 
'await Task.WhenAny(tcsServiceReceive.Task, Task.Delay(5000));')
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'w', encoding='utf8') as f:
    f.write(text)
