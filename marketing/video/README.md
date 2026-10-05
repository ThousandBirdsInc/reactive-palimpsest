# Palimpsest explainer video

`palimpsest-explainer.mp4` is a 2-minute, 1080p30 explainer for Palimpsest.
It covers the problem, the core idea, the architecture, and the advantages.

It is rendered frame by frame from `scenes.html`. Every frame is a pure
function of time (`window.render(t)`), so the output is deterministic.

```sh
# stills for layout checks
NODE_PATH=$(npm root -g) node render.mjs stills /tmp/stills 5 36 92
# full render (needs playwright + chromium and ffmpeg)
NODE_PATH=$(npm root -g) node render.mjs video silent.mp4 30
python3 soundtrack.py music.wav          # needs numpy
ffmpeg -i silent.mp4 -i music.wav -c:v copy -c:a aac -b:a 192k -shortest \
  -movflags +faststart palimpsest-explainer.mp4
```

Edit the copy or timing in `scenes.html` (`SCENES` holds the timeline).
Fonts (Inter, JetBrains Mono, OFL) are vendored in `fonts/`.
