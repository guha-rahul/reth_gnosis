use alloy_consensus::TxEip4844;
pub use gnosis_primitives::header::GnosisHeader;

pub type TransactionSigned = alloy_consensus::EthereumTxEnvelope<TxEip4844>;

// pub type GnosisHeader = alloy_consensus::Header;

/// The Block type of this node
pub type GnosisBlock = alloy_consensus::Block<TransactionSigned, GnosisHeader>;

/// The body type of this node
pub type BlockBody = alloy_consensus::BlockBody<TransactionSigned, GnosisHeader>;

/// Trait to convert a consensus block into a `GnosisBlock`
pub trait IntoBlock {
    fn into_block(self) -> GnosisBlock;
}

impl IntoBlock for alloy_consensus::Block<TransactionSigned, alloy_consensus::Header> {
    fn into_block(self) -> GnosisBlock {
        GnosisBlock {
            header: GnosisHeader::from(self.header),
            body: BlockBody {
                transactions: self.body.transactions,
                ommers: self
                    .body
                    .ommers
                    .into_iter()
                    .map(GnosisHeader::from)
                    .collect(),
                withdrawals: self.body.withdrawals,
            },
        }
    }
}
