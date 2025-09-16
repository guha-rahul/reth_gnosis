use alloy_sol_macro::sol;
use alloy_primitives::{address, Address};

pub const CHANNEL_CONTRACT_ADDR: Address = address!("0x693Bac5ce61c720dDC68533991Ceb41199D8F8ae");

sol! {
    // Core channel lifecycle
    event ChannelOpened(address indexed src, address indexed dest);
    event ChannelClosed(address indexed src, address indexed dest);

    // Balance changes
    event ChannelBalanceIncreased(address indexed src, address indexed dest, uint256 amount);
    event ChannelBalanceDecreased(address indexed src, address indexed dest, uint256 amount);

    // Closure flow
    event OutgoingChannelClosureInitiated(address indexed src, address indexed dest, uint32 closureInitiationTime);

    // EIP-712 domain separators
    event DomainSeparatorUpdated(bytes32 newDomainSeparator);
    event LedgerDomainSeparatorUpdated(bytes32 newLedgerDomainSeparator);

    // Ticket redemption
    event TicketRedeemed(
        address indexed src,
        address indexed dest,
        bytes32 nextCommitment,
        uint256 ticketEpoch,
        uint256 ticketIndex,
        bytes32 proofOfRelaySecret,
        uint256 amount,
        uint256 winProb,
        bytes   signature
    );
}

pub const ANNOUNCEMENTS: Address = address!("0x619eabE23FD0E2291B50a507719aa633fE6069b8");
sol! {
    event AddressAnnouncement(address indexed account, bytes publicKey, bytes multiaddr);
    event KeyBinding(address indexed account, bytes publicKey);
    event RevokeAnnouncement(address indexed account);
}

pub const NETWORK_REGISTRY: Address = address!("0x582b4b586168621dAf83bEb2AeADb5fb20F8d50d");
sol! {
    event Registered(address indexed account, string peerId);
    event RegisteredByManager(address indexed manager, address indexed account, string peerId);
    event Deregistered(address indexed account, string peerId);
    event DeregisteredByManager(address indexed manager, address indexed account, string peerId);
    event EligibilityUpdated(address indexed account, bool isEligible, uint256 rank);
    event RequirementUpdated(bytes32 key, uint256 value);
    event NetworkRegistryStatusUpdated(bool isPaused);
}

pub const NODE_SAFE_REGISTRY: Address = address!("0xe15C24a0910311c83aC78B5930d771089E93077b");
sol! {
    event RegisteredNodeSafe(address indexed owner, address safe);
    event DergisteredNodeSafe(address indexed owner, address safe);
    event NodeSafeDomainSeparatorUpdated(bytes32 newDomainSeparator);
}

pub const NODE_STAKE_V2_FACTORY: Address = address!("0x098B275485c406573D042848D66eb9d63fca311C");

pub const TICKET_PRICE_ORACLE: Address = address!("0xcA5656Fe6F2d847ACA32cf5f38E51D2054cA1273");
sol! { event TicketPriceUpdated(uint256 price); }

pub const WINNING_PROBABILITY_ORACLE: Address = address!("0x7Eb8d762fe794A108e568aD2097562cc5D3A1359");
sol! { event WinProbUpdated(uint256 winProb); }
