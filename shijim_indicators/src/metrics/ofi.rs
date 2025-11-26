use numpy::PyReadonlyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyclass]
pub struct RustOfiCalculator {
    prev_bid: Option<(f64, f64)>,
    prev_ask: Option<(f64, f64)>,
}

#[pymethods]
impl RustOfiCalculator {
    #[new]
    pub fn new() -> Self {
        Self {
            prev_bid: None,
            prev_ask: None,
        }
    }

    pub fn reset(&mut self) {
        self.prev_bid = None;
        self.prev_ask = None;
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_from_levels<'py>(
        &mut self,
        bid_prices: PyReadonlyArray1<'py, f64>,
        bid_sizes: PyReadonlyArray1<'py, f64>,
        ask_prices: PyReadonlyArray1<'py, f64>,
        ask_sizes: PyReadonlyArray1<'py, f64>,
    ) -> PyResult<Option<f64>> {
        let best_bid = Self::best_level(bid_prices.as_slice()?, bid_sizes.as_slice()?)?;
        let best_ask = Self::best_level(ask_prices.as_slice()?, ask_sizes.as_slice()?)?;

        if best_bid.is_none() || best_ask.is_none() {
            // Missing depth data; treat as zero flow and update stored state.
            self.prev_bid = best_bid;
            self.prev_ask = best_ask;
            return Ok(Some(0.0));
        }

        let bid = best_bid.unwrap();
        let ask = best_ask.unwrap();

        let prev_bid = match self.prev_bid {
            Some(prev) => prev,
            None => {
                self.prev_bid = Some(bid);
                self.prev_ask = Some(ask);
                return Ok(None);
            }
        };
        let prev_ask = match self.prev_ask {
            Some(prev) => prev,
            None => {
                self.prev_bid = Some(bid);
                self.prev_ask = Some(ask);
                return Ok(None);
            }
        };

        let bid_contrib = if bid.0 > prev_bid.0 {
            bid.1
        } else if bid.0 < prev_bid.0 {
            -prev_bid.1
        } else {
            bid.1 - prev_bid.1
        };

        let ask_contrib = if ask.0 < prev_ask.0 {
            ask.1
        } else if ask.0 > prev_ask.0 {
            -prev_ask.1
        } else {
            ask.1 - prev_ask.1
        };

        self.prev_bid = Some(bid);
        self.prev_ask = Some(ask);

        Ok(Some(bid_contrib - ask_contrib))
    }
}

impl RustOfiCalculator {
    fn best_level(prices: &[f64], sizes: &[f64]) -> PyResult<Option<(f64, f64)>> {
        if prices.is_empty() || sizes.is_empty() {
            return Ok(None);
        }
        if prices.len() != sizes.len() {
            return Err(PyValueError::new_err(
                "price/size arrays must have matching length",
            ));
        }
        Ok(Some((prices[0], sizes[0])))
    }
}
