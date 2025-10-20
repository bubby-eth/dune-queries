SELECT
  count(*) as swaps,
  sum(amount_usd) as volume,
  count(distinct tx_from) as traders
FROM
  dex.trades
WHERE
  token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  OR token_sold_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597