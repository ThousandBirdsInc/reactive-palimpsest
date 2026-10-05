"""Synthesizes the explainer's ambient soundtrack (pad + arpeggio + scene chimes)."""
import sys, wave
import numpy as np

SR, DUR = 44100, 122.0
t = np.arange(int(SR * DUR)) / SR
out = np.zeros_like(t)
midi = lambda m: 440.0 * 2 ** ((m - 69) / 12)

# A minor → F → C → G, 8 s per chord
CHORDS = [[57, 60, 64], [53, 57, 60], [48, 52, 55], [55, 59, 62]]
SEG = 8.0
def env_seg(i):
    a, b = i * SEG, (i + 1) * SEG
    return np.clip((t - a + 1.5) / 1.5, 0, 1) * np.clip((b + 1.5 - t) / 1.5, 0, 1)

nseg = int(np.ceil(DUR / SEG))
for i in range(nseg):
    e = env_seg(i)
    if not e.any():
        continue
    for m in CHORDS[i % 4]:
        for det in (-0.07, 0.07):
            f = midi(m - 12) * 2 ** (det / 12)
            out += e * 0.05 * (np.sin(2 * np.pi * f * t) + 0.35 * np.sin(4 * np.pi * f * t) + 0.12 * np.sin(6 * np.pi * f * t))
    root = CHORDS[i % 4][0]
    out += e * 0.07 * np.sin(2 * np.pi * midi(root - 24) * t)

# slow swell
out *= 0.75 + 0.25 * np.sin(2 * np.pi * t / 16.0 - np.pi / 2)

def pluck(start, m, amp, decay=3.5):
    n0 = int(start * SR); n = int(1.6 * SR)
    if n0 >= len(t): return
    n = min(n, len(t) - n0)
    tt = np.arange(n) / SR
    f = midi(m)
    out[n0:n0 + n] += amp * np.exp(-decay * tt) * (np.sin(2 * np.pi * f * tt) + 0.25 * np.sin(4 * np.pi * f * tt)) * np.clip(tt / 0.004, 0, 1)

# arpeggio: eighth notes at 112 bpm from 7 s to 114 s
step = 60 / 112 / 2
k = 0
s = 7.0
pattern = [0, 1, 2, 1, 2, 0, 1, 2]
while s < 114:
    ch = CHORDS[int(s // SEG) % 4]
    m = ch[pattern[k % 8]] + 12 + (12 if k % 16 in (6, 14) else 0)
    ramp = min(1, (s - 7) / 4, (114 - s) / 3)
    pluck(s, m, 0.045 * ramp * (1.0 if k % 2 == 0 else 0.6))
    s += step; k += 1

# scene-change chimes
for st in [7, 16.5, 28, 47, 56.5, 65.5, 75.5, 85, 95, 104.5, 114]:
    pluck(st, 81, 0.07, decay=1.6); pluck(st + 0.06, 88, 0.04, decay=1.8)
# diff ticks in "the idea" scene
for dt in [4.6, 5.9, 7.2, 8.4, 9.3]:
    pluck(16.5 + dt, 93, 0.05, decay=6)

# master fades + soft limiter
out *= np.clip(t / 2.0, 0, 1) * np.clip((DUR - t) / 4.0, 0, 1)
out = np.tanh(out * 1.6) / np.tanh(1.6) * 0.8
# simple stereo widening via short delay on right channel
d = int(0.012 * SR)
L = out; R = np.concatenate([np.zeros(d), out[:-d]])
st = (np.stack([L, R], 1) * 32767).astype(np.int16)
with wave.open(sys.argv[1], 'wb') as w:
    w.setnchannels(2); w.setsampwidth(2); w.setframerate(SR); w.writeframes(st.tobytes())
