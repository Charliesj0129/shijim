use numpy::PyReadonlyArray1;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::VecDeque;

const BUCKET_EPS: f64 = 1e-9;

#[pyclass]
pub struct RustVpinCalculator {
    bucket_volume: f64,
    window_size: usize,
    filled_volume: f64,
    buy_volume: f64,
    sell_volume: f64,
    imbalances: VecDeque<f64>,
    imbalance_sum: f64,
}

#[pymethods]
impl RustVpinCalculator {
    #[new]
    #[pyo3(text_signature = "(bucket_volume, window_size)")]
    pub fn new(bucket_volume: f64, window_size: usize) -> PyResult<Self> {
        if !bucket_volume.is_finite() || bucket_volume <= 0.0 {
            return Err(PyValueError::new_err(
                "bucket_volume must be a positive, finite number",
            ));
        }
        if window_size == 0 {
            return Err(PyValueError::new_err("window_size must be >= 1"));
        }
        Ok(Self {
            bucket_volume,
            window_size,
            filled_volume: 0.0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            imbalances: VecDeque::with_capacity(window_size),
            imbalance_sum: 0.0,
        })
    }

    pub fn reset(&mut self) {
        self.filled_volume = 0.0;
        self.buy_volume = 0.0;
        self.sell_volume = 0.0;
        self.imbalances.clear();
        self.imbalance_sum = 0.0;
    }

    pub fn update_signed_volume(&mut self, signed_volume: f64) -> PyResult<Option<f64>> {
        self.consume_trade(signed_volume)?;
        Ok(self.current_vpin())
    }

    pub fn update_signed_series<'py>(
        &mut self,
        signed_volumes: PyReadonlyArray1<'py, f64>,
    ) -> PyResult<Vec<Option<f64>>> {
        let slice = signed_volumes.as_slice()?;
        let mut out = Vec::with_capacity(slice.len());
        for &value in slice {
            out.push(self.update_signed_volume(value)?);
        }
        Ok(out)
    }

    pub fn buckets_ready(&self) -> usize {
        self.imbalances.len()
    }

    pub fn bucket_volume(&self) -> f64 {
        self.bucket_volume
    }
}

impl RustVpinCalculator {
    fn consume_trade(&mut self, signed_volume: f64) -> PyResult<()> {
        if !signed_volume.is_finite() {
            return Err(PyValueError::new_err(
                "signed_volume must be a finite float",
            ));
        }
        if signed_volume == 0.0 {
            return Ok(());
        }

        let direction_is_buy = signed_volume > 0.0;
        let mut remaining = signed_volume.abs();

        while remaining > 0.0 {
            if self.bucket_is_full() {
                self.finalize_bucket();
                continue;
            }

            let space = (self.bucket_volume - self.filled_volume).max(0.0);
            let take = remaining.min(space);
            if take <= 0.0 {
                // Defensive: space can only be zero if numerical drift made the bucket "full".
                self.finalize_bucket();
                continue;
            }

            if direction_is_buy {
                self.buy_volume += take;
            } else {
                self.sell_volume += take;
            }
            self.filled_volume += take;
            remaining -= take;

            if self.bucket_is_full() {
                self.finalize_bucket();
            }
        }

        Ok(())
    }

    fn bucket_is_full(&self) -> bool {
        self.bucket_volume - self.filled_volume <= BUCKET_EPS
    }

    fn finalize_bucket(&mut self) {
        if self.filled_volume <= 0.0 {
            return;
        }
        let imbalance = (self.buy_volume - self.sell_volume).abs();
        self.imbalances.push_back(imbalance);
        self.imbalance_sum += imbalance;
        if self.imbalances.len() > self.window_size {
            if let Some(old) = self.imbalances.pop_front() {
                self.imbalance_sum -= old;
            }
        }
        self.buy_volume = 0.0;
        self.sell_volume = 0.0;
        self.filled_volume = 0.0;
    }

    fn current_vpin(&self) -> Option<f64> {
        if self.imbalances.len() < self.window_size {
            return None;
        }
        let denom = self.bucket_volume * self.window_size as f64;
        Some(self.imbalance_sum / denom)
    }
}
