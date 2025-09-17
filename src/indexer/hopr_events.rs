use alloy_sol_macro::sol;
use alloy_primitives::{address, Address};

pub const CHANNEL_CONTRACT_ADDR: Address = address!("0x693Bac5ce61c720dDC68533991Ceb41199D8F8ae");

sol! {
    event ChannelBalanceDecreased(bytes32 indexed channelId, uint96 newBalance);
    event ChannelBalanceIncreased(bytes32 indexed channelId, uint96 newBalance);
    event ChannelClosed(bytes32 indexed channelId);
    event ChannelOpened(address indexed source, address indexed destination);
    event DomainSeparatorUpdated(bytes32 indexed domainSeparator);
    event OutgoingChannelClosureInitiated(bytes32 indexed channelId, uint32 closureTime);
    event LedgerDomainSeparatorUpdated(bytes32 indexed ledgerDomainSeparator);
    event TicketRedeemed(bytes32 indexed channelId, uint48 newTicketIndex);
}

pub const ANNOUNCEMENTS: Address = address!("0x619eabE23FD0E2291B50a507719aa633fE6069b8");
sol! {
    event AddressAnnouncement(address node, string baseMultiaddr);
    event KeyBinding(bytes32 ed25519_sig_0, bytes32 ed25519_sig_1, bytes32 ed25519_pub_key, address chain_key);
    event RevokeAnnouncement(address node);
}

pub const NETWORK_REGISTRY: Address = address!("0x582b4b586168621dAf83bEb2AeADb5fb20F8d50d");
sol! {
    event Deregistered(address indexed stakingAccount, address indexed nodeAddress);
    event DeregisteredByManager(address indexed stakingAccount, address indexed nodeAddress);
    event EligibilityUpdated(address indexed stakingAccount, bool indexed eligibility);
    event NetworkRegistryStatusUpdated(bool indexed isEnabled);
    event Registered(address indexed stakingAccount, address indexed nodeAddress);
    event RegisteredByManager(address indexed stakingAccount, address indexed nodeAddress);
    event RequirementUpdated(address indexed requirementImplementation);
}

pub const NODE_SAFE_REGISTRY: Address = address!("0xe15C24a0910311c83aC78B5930d771089E93077b");
sol! {
    event DergisteredNodeSafe(address indexed safeAddress, address indexed nodeAddress);
    event RegisteredNodeSafe(address indexed safeAddress, address indexed nodeAddress);
}

pub const NODE_STAKE_V2_FACTORY: Address = address!("0x098B275485c406573D042848D66eb9d63fca311C");

pub const TICKET_PRICE_ORACLE: Address = address!("0xcA5656Fe6F2d847ACA32cf5f38E51D2054cA1273");
sol! {
    event TicketPriceUpdated(uint256, uint256);
}
pub const WINNING_PROBABILITY_ORACLE: Address = address!("0x7Eb8d762fe794A108e568aD2097562cc5D3A1359");
sol! {
    event WinProbUpdated(uint56 oldWinProb, uint56 newWinProb);
}