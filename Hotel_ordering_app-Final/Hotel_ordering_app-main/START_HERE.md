# 🎯 START HERE - Deploy Your Hotel App to Render

**GitHub Repository**: `https://github.com/omkardhamane637-lang/Hotel_ordering_app.git`

---

## 🚀 3 Easy Steps to Deploy

### Step 1️⃣: Get Your Code to GitHub

Since you have the repository URL ready, follow these steps:

#### Option A: Using Git (If you have it installed)
```bash
git remote add origin https://github.com/omkardhamane637-lang/Hotel_ordering_app.git
git branch -M main
git push -u origin main
```

#### Option B: GitHub Desktop (Recommended if no Git installed)
1. Download: https://desktop.github.com/
2. File → New Repository
3. Name: `Hotel_ordering_app`
4. Local Path: `C:\Users\usery\.gemini\antigravity\scratch\hotel_app`
5. Commit all files and Publish to the specific GitHub URL.

#### Option C: Upload via Web
1. Go to https://github.com/omkardhamane637-lang/Hotel_ordering_app
2. Click "Upload files"
3. Drag all files from `hotel_app` folder
4. Commit changes

---

### Step 2️⃣: Deploy on Render

1. Go to https://dashboard.render.com/
2. Sign in with GitHub
3. Click **"New +"** → **"Blueprint"**
4. Find and select `Hotel_ordering_app`
5. Click **"Connect"**
6. Click **"Apply"**
7. Wait 2-5 minutes ⏱️

> [!WARNING]
> **Data Persistence (Free Tier)**: Render's free tier does not support persistent disks. This means your data (orders, products) **will be lost** every time the app restarts or redeploys. To keep your data, you would need to upgrade to Render's "Starter" plan ($7/month) and send me a message so I can re-add the disk configuration.

---

### Step 3️⃣: Access Your Live App! 🎉

1. Render will show the URL once deployment is finished.
2. Login with:
   - Email: `admin@hotel.com` (or your custom value)
   - Password: `admin123` (or your custom value)

---

## 📋 Quick Checklist

- [ ] Upload code to GitHub (`https://github.com/omkardhamane637-lang/Hotel_ordering_app.git`)
- [ ] Verify `render.yaml` is in the root
- [ ] Create Blueprint on Render
- [ ] Deploy and test login
