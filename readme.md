python script.py input.mkv output/ --best-quality --explicit-qualities=high,low
# 1️⃣ NVIDIA GeForce (RTX 3060, 4090, etc.) - Consumer cards
# Parallel doesn't help much, sessions will queue
python script.py input.mkv output/ --hw-accel=nvenc --explicit-qualities=high
# OR encode sequentially
python script.py input.mkv output/ --hw-accel=nvenc  # no --parallel


# 2️⃣ NVIDIA Quadro/Tesla - Workstation cards
# Parallel helps a LOT!
python script.py input.mkv output/ --hw-accel=nvenc --parallel  # ✅ Great!

# 3️⃣ Intel Quick Sync / AMD
# Parallel helps!
python script.py input.mkv output/ --hw-accel=qsv --parallel  # ✅ Good!
python script.py input.mkv output/ --hw-accel=amf --parallel  # ✅ Good!

# 4️⃣ No GPU (CPU only)
# Parallel helps the MOST!
python script.py input.mkv output/ --parallel  # ✅ Excellent!

# 5️⃣ Apple Silicon
# Sequential is better
python script.py input.mkv output/ --hw-accel=videotoolbox  # no --parallel

--hw-accel=auto