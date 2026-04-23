import os
with open('D:/Code/CSharp/RouterController/RouterController/Test/TransactionSystemIntegrationTests.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('public async Task Heartbeat_Should_Update_To_Redis_SortedSet()',
'''public async Task Heartbeat_Should_Update_To_Redis_SortedSet()''')
print('Replace done')
