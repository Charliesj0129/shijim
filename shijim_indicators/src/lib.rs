use pyo3::prelude::*;

pub mod metrics;
pub use metrics::hawkes::RustHawkesIntensity;
pub use metrics::ofi::RustOfiCalculator;
pub use metrics::vpin::RustVpinCalculator;

#[pymodule]
fn shijim_indicators(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RustOfiCalculator>()?;
    m.add_class::<RustVpinCalculator>()?;
    m.add_class::<RustHawkesIntensity>()?;
    Ok(())
}
