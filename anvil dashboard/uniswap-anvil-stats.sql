SELECT
  count(*) as swaps,
  sum(amount_usd) as volume,
  count(distinct tx_from) as traders
FROM
  dex.trades
WHERE
  token_bought_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
  OR token_sold_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc