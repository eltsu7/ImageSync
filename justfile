set shell := ["bash", "-uc"]

test_root := justfile_directory() + "/.test"
test_mount_root := test_root + "/mounts"

install:
    cargo install --path crates/imagesync-cli --locked --force

test-input-refresh input:
    rm -rf "{{test_root}}/input"
    mkdir -p "{{test_root}}/input/DCIM"
    cp --reflink=auto "{{input}}"/*.arw "{{test_root}}/input/DCIM/"

test-tui: _test-output-reset _test-device _test-config
    rm -f "{{test_root}}"/input/DCIM/SKIP_*.arw
    # Keep the original fixture files as copy candidates. Add 30 distinct
    # source files with matching destination copies to exercise pre-skip.
    files=("{{test_root}}"/input/DCIM/*.arw); for i in {0..29}; do source="${files[i % ${#files[@]}]}"; name="$(printf 'SKIP_%03d.arw' "$i")"; cp --reflink=auto "$source" "{{test_root}}/input/DCIM/$name"; cp --reflink=auto "$source" "{{test_root}}/output/images/$name"; done
    test -n "$(printf '%s\n' "{{test_root}}/input/DCIM/"*.arw)"
    IMAGESYNC_MOUNT_ROOT="{{test_mount_root}}" cargo run -q -p imagesync-cli -- --config "{{test_root}}/config.toml" tui

test-tui-keep: _test-output _test-device _test-config
    test -n "$(printf '%s\n' "{{test_root}}/input/DCIM/"*.arw)"
    IMAGESYNC_MOUNT_ROOT="{{test_mount_root}}" cargo run -q -p imagesync-cli -- --config "{{test_root}}/config.toml" tui

_test-device:
    rm -rf "{{test_mount_root}}"
    mkdir -p "{{test_mount_root}}"
    ln -s "{{test_root}}/input" "{{test_mount_root}}/Test-Camera"

_test-config:
    printf '%s\n' '[catalogues.Test]' 'images_root = "{{test_root}}/output/images"' 'videos_root = "{{test_root}}/output/videos"' 'images_template = "{yyyy}/{yyyy}-{mm}-{dd}"' 'videos_template = "{yyyy}/{yyyy}-{mm}-{dd}"' '' '[filters]' 'raw_mode = "raw_only"' 'include_videos = false' 'include_sidecars = false' '' '[performance]' 'scan_workers = 0' 'metadata_batch_size = 20' 'copy_workers = 4' '' '[verify]' 'enabled = false' 'algorithm = "xxh3"' > "{{test_root}}/config.toml"

_test-output-reset:
    rm -rf "{{test_root}}/output"
    mkdir -p "{{test_root}}/output/images" "{{test_root}}/output/videos"

_test-output:
    mkdir -p "{{test_root}}/output/images" "{{test_root}}/output/videos"
