import os
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'r', encoding='utf8') as f:
    text = f.read()
text = text.replace('await Task.Delay(5000, ct);', '''await Task.Delay(1000, ct);
                var cutoffScore = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10;
                if (_redisDb != null) await _redisDb.SortedSetRemoveRangeByScoreAsync("active_players_ts", double.NegativeInfinity, cutoffScore);''')
with open('D:/Code/CSharp/RouterController/RouterController/TransactionRouter/TransactionServiceClient.cs', 'w', encoding='utf8') as f:
    f.write(text)
