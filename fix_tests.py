import os
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('public TransactionRouterClient(string[] natsUrl, int id)', 'public TransactionRouterClient(string[] natsUrl, int id)')
