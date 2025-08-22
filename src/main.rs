use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::VecDeque;

// Assume timestamp as nanoseconds since custom epoch
static GLOBAL_TIMESTAMP: AtomicU64 = AtomicU64::new(1);

fn next_timestamp() -> u64 {
    GLOBAL_TIMESTAMP.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub price: u64,
    pub quantity: u64,
    pub maker_id: u64,
    pub taker_id: u64,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: u64,
    pub price: u64,
    pub quantity: u64,
    pub timestamp: u64,
}


#[derive(Debug)]
pub struct PriceLevel {
    pub orders: VecDeque<Order>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct HeapEntry {
    price: u64,
}

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Reverse;

pub struct OrderBook {
    buy_heap: BinaryHeap<HeapEntry>,
    sell_heap: BinaryHeap<Reverse<HeapEntry>>,
    buy_map: HashMap<u64, PriceLevel>,
    sell_map: HashMap<u64, PriceLevel>,
    trade_buffer: Vec<Trade>,
}

impl OrderBook {
    fn get_quantity_at_price(price_map: &HashMap<u64, PriceLevel>,  price: u64) -> Option<(u64, u64)> {
        price_map.get(&price).map(|level| {
            let total_qty = level.orders.iter().map(|o| o.quantity).sum();
            (price, total_qty)
        })
    }

    pub fn buy_at(&self, price: u64) -> Option<(u64, u64)> {
        OrderBook::get_quantity_at_price(&self.buy_map, price)
    }

    pub fn sell_at(&self, price: u64) -> Option<(u64, u64)> {
        OrderBook::get_quantity_at_price(&self.sell_map, price)
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            buy_heap: BinaryHeap::with_capacity(1024),
            sell_heap: BinaryHeap::with_capacity(1024),
            buy_map: HashMap::with_capacity(1024),
            sell_map: HashMap::with_capacity(1024),
            trade_buffer: Vec::with_capacity(128),
        }
    }

    pub fn place_order(&mut self, side: Side, price: u64, quantity: u64, id: u64) -> &[Trade] {
        if quantity == 0 {
            self.trade_buffer.clear();
            return &self.trade_buffer;
        }

        let timestamp = next_timestamp();
        let mut remaining_quantity = quantity;
        self.trade_buffer.clear();

        match side {
            Side::Buy => {
                // Buy order matches against sell_heap/sell_map
                while remaining_quantity > 0 {
                    let best_price = self.sell_heap.peek().map(|p| p.0.price);
                    if let Some(best_price) = best_price {
                        if price < best_price {
                            break;
                        }
                        let level = self.sell_map.get_mut(&best_price).unwrap();
                        Self::match_level(level, best_price, &mut remaining_quantity, id, &mut self.trade_buffer);

                        // remove this price level if empty
                        if self.sell_map.get(&best_price).map_or(true, |lvl| lvl.orders.is_empty()) {
                            self.sell_map.remove(&best_price);
                            self.sell_heap.pop();
                        }
                    } else {
                        break;
                    }
                }
                if remaining_quantity > 0 {
                    let order = Order { id, price, quantity: remaining_quantity, timestamp };
                    let level = self.buy_map.entry(price).or_insert_with(|| PriceLevel {
                        orders: VecDeque::with_capacity(8),
                    });
                    level.orders.push_back(order);
                    if !self.buy_heap.iter().any(|e| e.price == price) {
                        self.buy_heap.push(HeapEntry { price });
                    }
                }
            }
            Side::Sell => {
                // Sell order matches against buy_heap/buy_map
                while remaining_quantity > 0 {
                    let best_price = self.buy_heap.peek().map(|p| p.price);
                    if let Some(best_price) = best_price {
                        if price > best_price {
                            break;
                        }
                        let level = self.buy_map.get_mut(&best_price).unwrap();
                        Self::match_level(level, best_price, &mut remaining_quantity, id, &mut self.trade_buffer);

                        // remove this price level if empty
                        if self.buy_map.get(&best_price).map_or(true, |lvl| lvl.orders.is_empty()) {
                            self.buy_map.remove(&best_price);
                            self.buy_heap.pop();
                        }
                    } else {
                        break;
                    }
                }
                if remaining_quantity > 0 {
                    let order = Order { id, price, quantity: remaining_quantity, timestamp };
                    let level = self.sell_map.entry(price).or_insert_with(|| PriceLevel {
                        orders: VecDeque::with_capacity(8),
                    });
                    level.orders.push_back(order);
                    if !self.sell_heap.iter().any(|e| e.0.price == price) {
                        self.sell_heap.push(Reverse(HeapEntry { price }));
                    }
                }
            }
        }
        &self.trade_buffer
    }

    fn match_level(
        level: &mut PriceLevel,
        price: u64,
        remaining_quantity: &mut u64,
        taker_id: u64,
        trades: &mut Vec<Trade>,
    ) {
        println!("Before match_level, price level {:?}", level);

        while let Some(order) = level.orders.front_mut() {
            let trade_qty = order.quantity.min(*remaining_quantity);
            trades.push(Trade {
                price,
                quantity: trade_qty,
                maker_id: order.id,
                taker_id,
            });

            order.quantity -= trade_qty;
            *remaining_quantity -= trade_qty;

            if order.quantity == 0 {
                level.orders.pop_front();
            }

            if *remaining_quantity == 0 {
                break;
            }
        }

        println!("After match_level, price level {:?}", level);
    }

    pub fn best_buy(&self) -> Option<(u64, u64)> {
        self.buy_heap.peek().and_then(|entry| {
            self.buy_map.get(&entry.price).map(|level| {
                let total_qty = level.orders.iter().map(|o| o.quantity).sum();
                (entry.price, total_qty)
            })
        })
    }

    pub fn best_sell(&self) -> Option<(u64, u64)> {
        self.sell_heap.peek().and_then(|Reverse(entry)| {
            self.sell_map.get(&entry.price).map(|level| {
                let total_qty = level.orders.iter().map(|o| o.quantity).sum();
                (entry.price, total_qty)
            })
        })
    }
}

fn main() {
    println!("OrderBook test: cargo test");
}


#[test]
fn test_basic_match() {
    let mut ob = OrderBook::new();

    assert_eq!(ob.place_order(Side::Buy, 10, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 9, 200, 2).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 8, 300, 3).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 7, 400, 4).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 8, 500, 5).len(), 0);

    assert_eq!(ob.place_order(Side::Sell, 11, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Sell, 12, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Sell, 13, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Sell, 14, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Sell, 15, 100, 1).len(), 0);

    assert_eq!(ob.place_order(Side::Sell, 10, 100, 1).len(), 1);
    assert_eq!(ob.place_order(Side::Sell, 10, 100, 2).len(), 0);
    assert_eq!(ob.place_order(Side::Sell, 8,  300, 2).len(), 2);
    assert_eq!(ob.place_order(Side::Sell, 8,  100, 3).len(), 1);

}

#[test]
fn test_fifo_priority() {
    let mut ob = OrderBook::new();

    assert_eq!(ob.place_order(Side::Buy, 10, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 10, 200, 2).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 10, 300, 3).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 9, 400, 4).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 9, 500, 5).len(), 0);

    let trades = ob.place_order(Side::Sell, 10, 600, 10);

    assert_eq!(trades.len(), 3);
    assert_eq!(trades[0].maker_id, 1);
    assert_eq!(trades[1].maker_id, 2);
    assert_eq!(trades[2].maker_id, 3);
}

#[test]
fn test_partial_fill() {
    let mut ob = OrderBook::new();

    assert_eq!(ob.place_order(Side::Buy, 10, 100, 1).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 10, 200, 2).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 10, 300, 3).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 9, 400, 4).len(), 0);
    assert_eq!(ob.place_order(Side::Buy, 9, 500, 5).len(), 0);

    println!("First partial fill");
    let trades = ob.place_order(Side::Sell, 10, 199, 10);

    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].maker_id, 1);
    assert_eq!(trades[0].quantity, 100);
    assert_eq!(trades[1].maker_id, 2);
    assert_eq!(trades[1].quantity, 99);

    println!("Second partial fill");
    let trades = ob.place_order(Side::Sell, 10, 199, 11);
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].maker_id, 2);
    assert_eq!(trades[0].quantity, 101);
    assert_eq!(trades[1].maker_id, 3);
    assert_eq!(trades[1].quantity, 98);
}


#[test]
fn test_buy_at_and_sell_at() {
    let mut ob = OrderBook::new();

    ob.place_order(Side::Buy, 10, 100, 1);
    ob.place_order(Side::Buy, 10, 200, 2);
    ob.place_order(Side::Buy, 9, 300, 3);

    ob.place_order(Side::Sell, 11, 150, 4);
    ob.place_order(Side::Sell, 11, 50, 5);
    ob.place_order(Side::Sell, 12, 100, 6);

    assert_eq!(ob.buy_at(10), Some((10, 300))); // 100 + 200
    assert_eq!(ob.buy_at(9), Some((9, 300)));
    assert_eq!(ob.buy_at(8), None);

    assert_eq!(ob.sell_at(11), Some((11, 200))); // 150 + 50
    assert_eq!(ob.sell_at(12), Some((12, 100)));
    assert_eq!(ob.sell_at(13), None);
}
