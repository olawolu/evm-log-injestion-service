function transform(block) {
  const TRANSFER_EVENTS = [
    "event Transfer(address indexed from, address indexed to, uint256 value)",
  ];

  const events = [];
  const swaps = [];

  for (const tx of block.transactions) {
    if (!tx.receipt) continue;

    for (let i = 0; i < tx.receipt.logs.length; i += 1) {
      try {
        const log = tx.receipt.logs[i];

        const { metadata, decoded } = utils.evmDecodeLogWithMetadata(
          log,
          TRANSFER_EVENTS,
        );

        if (metadata && decoded) {
          const method = metadata.name.split(" ").pop();
          const timestamp = new Date(block.timestamp * 1000).toISOString();

          // track all events
          events.push({
            contract_address: log.address.toLowerCase(),
            transaction_hash: tx.hash,
            log_index: log.logIndex,
            method,
            timestamp,
            decoded,
          });

        }
      } catch (e) {
        // pass by unmatched logs
      }
    }
  }

  return {
    events: events,
  };
}
