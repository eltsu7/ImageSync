set shell := ["bash", "-uc"]

test_root := justfile_directory() + "/.test"

test-input-refresh input:
    rm -rf "{{test_root}}/input"
    mkdir -p "{{test_root}}/input/DCIM"
    cp --reflink=auto "{{input}}"/*.arw "{{test_root}}/input/DCIM/"

test-tui: _test-output-reset
    rm -f "{{test_root}}"/input/DCIM/SKIP_*.arw
    # Keep the original fixture files as copy candidates. Add 30 distinct
    # source files with matching destination copies to exercise pre-skip.
    files=("{{test_root}}"/input/DCIM/*.arw); for i in {0..29}; do source="${files[i % ${#files[@]}]}"; name="$(printf 'SKIP_%03d.arw' "$i")"; cp --reflink=auto "$source" "{{test_root}}/input/DCIM/$name"; cp --reflink=auto "$source" "{{test_root}}/output/images/$name"; done
    test -n "$(printf '%s\n' "{{test_root}}/input/DCIM/"*.arw)"
    cargo run -q -p imagesync-cli -- --config "{{test_root}}/config.toml" tui --source "{{test_root}}/input"

test-tui-keep: _test-output
    test -n "$(printf '%s\n' "{{test_root}}/input/DCIM/"*.arw)"
    cargo run -q -p imagesync-cli -- --config "{{test_root}}/config.toml" tui --source "{{test_root}}/input"

_test-output-reset:
    rm -rf "{{test_root}}/output"
    mkdir -p "{{test_root}}/output/images" "{{test_root}}/output/videos"

_test-output:
    mkdir -p "{{test_root}}/output/images" "{{test_root}}/output/videos"
