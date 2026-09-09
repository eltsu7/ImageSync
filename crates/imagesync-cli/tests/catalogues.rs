use std::{fs, path::Path, process::Command};

fn imagesync(config: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_imagesync"));
    command.arg("--config").arg(config);
    command
}

fn write_catalogues(config: &Path, root: &Path) {
    let alpha_images = root.join("alpha-images");
    let alpha_videos = root.join("alpha-videos");
    let zebra_images = root.join("zebra-images");
    let zebra_videos = root.join("zebra-videos");
    fs::write(
        config,
        format!(
            r#"[catalogues.Zebra]
images_root = "{}"
videos_root = "{}"
images_template = "{{yyyy}}/z-images"
videos_template = "{{yyyy}}/z-videos"

[catalogues.Alpha]
images_root = "{}"
videos_root = "{}"
images_template = "{{yyyy}}/a-images"
videos_template = "{{yyyy}}/a-videos"
"#,
            zebra_images.display(),
            zebra_videos.display(),
            alpha_images.display(),
            alpha_videos.display(),
        ),
    )
    .unwrap();
}

#[test]
fn catalogues_lists_all_routing_fields_in_stable_order_without_exiftool() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_catalogues(&config, dir.path());

    let output = imagesync(&config)
        .arg("catalogues")
        .env("PATH", "")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        stdout,
        format!(
            "Alpha\n  images root: {}\n  images folders: {{yyyy}}/a-images\n  videos root: {}\n  videos folders: {{yyyy}}/a-videos\n\nZebra\n  images root: {}\n  images folders: {{yyyy}}/z-images\n  videos root: {}\n  videos folders: {{yyyy}}/z-videos\n",
            dir.path().join("alpha-images").display(),
            dir.path().join("alpha-videos").display(),
            dir.path().join("zebra-images").display(),
            dir.path().join("zebra-videos").display(),
        )
    );
}

#[test]
fn catalogues_reports_empty_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("missing.toml");
    let output = imagesync(&config).arg("catalogues").output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "No catalogues configured.\n"
    );
}

#[test]
fn scan_and_sync_require_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("missing.toml");
    for subcommand in ["scan", "sync"] {
        let output = imagesync(&config)
            .args([subcommand, dir.path().to_str().unwrap()])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("--catalogue <CATALOGUE>"),
            "{subcommand}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn unknown_catalogue_fails_before_exiftool_and_lists_names() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    write_catalogues(&config, dir.path());
    let output = imagesync(&config)
        .args([
            "scan",
            "--catalogue",
            "Missing",
            dir.path().to_str().unwrap(),
        ])
        .env("PATH", "")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("catalogue \"Missing\" not found; available: Alpha, Zebra"),
        "{stderr}"
    );
    assert!(!stderr.contains("exiftool not found"), "{stderr}");
}

#[test]
fn removed_routing_flags_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("missing.toml");
    for flag in [
        "--images-root",
        "--videos-root",
        "--images-template",
        "--videos-template",
    ] {
        let output = imagesync(&config)
            .args([
                "scan",
                "--catalogue",
                "Photos",
                flag,
                "obsolete",
                dir.path().to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unexpected argument"),
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
