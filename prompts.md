# ImageSync – User Prompts

## Prompt 1

lets build a program that will automatically copy images from camera to folder structure to some drive. currently i have a sony camera, and i want images to a folder with yyyy/yyyy-mm-dd/ folders. the dates should come from the image metadata, and i want to control the root folder of images and videos separately. i also want to include/exclude non-raw images. currently i have a sony camera, but i would like for this to work on all cameras. cameras have different folder structure on sd card and different raw image formats.

i want this to work on my machine, but i want to release this as an open source project so others can use this as well. so it would have to expand to other cameras and folder formats as well. for the first version, only TUI is needed. it should sync all images to a folder that don't already exist in there. so no overwriting.

figure out the tech stack first, feel free to ask questions.

---

## Prompt 2

start with sony profile only, stash others as todo somewhere. mtp/ptp needs to be in the first version as i will not use raw sd card reader. try to separate the TUI from the actual logic, as i might want to upgrade to actual GUI later.

---

## Prompt 3

added git repo. mit license. also, the copy path should be configurable by user. i want yyyy/yyyy-mm-dd/. some may want yyyy/mm/dd, or so on. after these changes, start implementing.

---

## Prompt 4

seems like it tries to overwrite. DON'T COPY ANYTHING!!! but, hardware is now in place in /run/media/eeli/CC49-31C9/, and target folder is /media/Pictures/. 

all pictures should already be there, but the scan command gave me this:

~/Koodit/ImageSync (main)
❯ ./target/release/imagesync scan --images-root /media/Pictures/ --videos-root /home/eeli/Documents/temp/ /run/media/eeli/CC49-31C9/
scanning: CC49-31C9
  1238 files enumerated
  metadata: 200/1232
  metadata: 400/1232
  metadata: 600/1232
  metadata: 800/1232
  metadata: 1000/1232
  metadata: 1200/1232
  metadata: 1232/1232
  planned: 1232 copies, 0 skips, 0 errors

=== Plan ===
  copies: 1232   skips: 0   errors: 0

  copy             1232

  COPY         DCIM/100MSDCF/DSC04290.ARW -> /media/Pictures/2026/2026-03-14/DSC04290.ARW [2026-03-14 23:20:37]
  COPY         DCIM/100MSDCF/DSC04290.JPG -> /media/Pictures/2026/2026-03-14/DSC04290.JPG [2026-03-14 23:20:37]
  COPY         DCIM/100MSDCF/DSC04291.ARW -> /media/Pictures/2026/2026-03-16/DSC04291.ARW [2026-03-16 06:13:12]
  COPY         DCIM/100MSDCF/DSC04291.JPG -> /media/Pictures/2026/2026-03-16/DSC04291.JPG [2026-03-16 06:13:12]
  COPY         DCIM/100MSDCF/DSC04292.ARW -> /media/Pictures/2026/2026-03-16/DSC04292.ARW [2026-03-16 06:13:20]
  COPY         DCIM/100MSDCF/DSC04292.JPG -> /media/Pictures/2026/2026-03-16/DSC04292.JPG [2026-03-16 06:13:20]
  COPY         DCIM/100MSDCF/DSC04293.ARW -> /media/Pictures/2026/2026-03-16/DSC04293.ARW [2026-03-16 06:13:38]
  COPY         DCIM/100MSDCF/DSC04293.JPG -> /media/Pictures/2026/2026-03-16/DSC04293.JPG [2026-03-16 06:13:38]
  COPY         DCIM/100MSDCF/DSC04294.ARW -> /media/Pictures/2026/2026-03-16/DSC04294.ARW [2026-03-16 06:15:12]
  COPY         DCIM/100MSDCF/DSC04294.JPG -> /media/Pictures/2026/2026-03-16/DSC04294.JPG [2026-03-16 06:15:12]
  COPY         DCIM/100MSDCF/DSC04295.ARW -> /media/Pictures/2026/2026-03-16/DSC04295.ARW [2026-03-16 06:15:13]

---

## Prompt 5

it's not compiling

---

## Prompt 6

go ahead with the TUI

---

## Prompt 7

go ahead with the TUI and mount auto detect

---

## Prompt 8

if output folders are not in config, i should be able to input them to the TUI

---

## Prompt 9

the progress bar isn't progressing. same thing happens in the cli, the metadata progress isn't updated until everything is done.

---

## Prompt 10

the progress seems to work in batches of 200. can you make it 50, so it's a bit more responsive, even though a bit slower? or is it? anyway, i also want to choose the raw option in the TUI.

---

## Prompt 11

the tui still only updates at 200 metadata steps.

---

## Prompt 12

in the preview folder, just show a treeview of the images that will be copied to the output folder. so the output folder tree, but with only the new files. don't show every file, only 3 first per folder and then (...). don't show skipped or filtered here. show a + sign or something on completely new folders that will be created. for every folder, display a count of raw images and non-raw images separately.

---

## Prompt 13

some problems. when copying, the progress bar for a file seems to stutter alot. also, when i canceled the copy, it said it copied 0, even though it got halfway throught the files. also, this seems very slow, can we copy multiple images at a time? check how rapidphotodownloader handles this as it seems very fast.

---

## Prompt 14

write a documentation of the architecture for us both. also write a todo list and add item: speedup the metadata/image discovery. another todo item: change raw-mode after scan and update the preview. don't do these yet, only document.

---

