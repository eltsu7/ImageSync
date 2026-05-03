//! `imagesync-tui` — ratatui frontend.
//!
//! M3 milestone. The crate currently exposes only a placeholder so the CLI
//! can build with the `tui` feature enabled. Real screens land in M3.

use anyhow::Result;

/// Run the TUI. Stub for now — prints a notice and exits.
pub async fn run() -> Result<()> {
    eprintln!("imagesync-tui: not implemented yet (planned for M3).");
    eprintln!("Use `imagesync scan <path>` or `imagesync sync <path>` for now.");
    Ok(())
}
