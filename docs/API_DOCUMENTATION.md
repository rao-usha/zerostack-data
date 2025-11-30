# 📖 API Documentation Quick Start

## 🌐 Access Your API Documentation

Your Nexdata API comes with **automatic, interactive documentation**!

### Swagger UI (Interactive)
**👉 http://localhost:8000/docs**

- ✅ Try APIs instantly in your browser
- ✅ See live request/response examples
- ✅ Browse all 50+ endpoints
- ✅ Copy curl commands
- ✅ Download OpenAPI spec

### ReDoc (Read-only)
**👉 http://localhost:8000/redoc**

- ✅ Clean documentation view
- ✅ Better for reading/printing
- ✅ Search functionality

### OpenAPI Schema (JSON)
**👉 http://localhost:8000/openapi.json**

- ✅ Import into Postman/Insomnia
- ✅ Generate client libraries
- ✅ API testing automation

---

## 🚀 Quick Start

1. **Start the service:**
   ```bash
   python scripts/start_service.py
   ```

2. **Open your browser to:**
   ```
   http://localhost:8000/docs
   ```

3. **Try your first API call!**

---

## 📚 What's Documented?

### All 10 Data Sources:
- 📊 **Census** - Demographics, housing, economic data
- 💰 **FRED** - 800K+ economic time series
- ⚡ **EIA** - Energy production and prices
- 🏛️ **SEC** - Company financials, Form ADV
- 🌦️ **NOAA** - Weather and climate data
- 🏠 **Real Estate** - Zillow home values
- 🗺️ **GeoJSON** - Geographic boundaries
- 💼 **Family Offices** - Investment adviser tracking
- ⚙️ **Jobs** - Ingestion job management
- 🏥 **Health** - Service health checks

### 50+ Endpoints Documented

Every endpoint includes:
- Request parameters and body schema
- Response format and examples
- Error codes and descriptions
- Interactive "Try it out" functionality

---

## 💡 Common Tasks

### Test an Endpoint
1. Go to http://localhost:8000/docs
2. Find your endpoint
3. Click "Try it out"
4. Edit parameters
5. Click "Execute"

### Import to Postman
1. Open Postman
2. Click "Import"
3. Paste: `http://localhost:8000/openapi.json`
4. Done!

### Copy Curl Command
1. Execute any request in Swagger UI
2. Scroll to "Curl" section
3. Copy the command
4. Run in terminal

### Generate Client Library
```bash
# Python
pip install openapi-python-client
openapi-python-client generate --url http://localhost:8000/openapi.json

# TypeScript
npm install -g @openapitools/openapi-generator-cli
openapi-generator-cli generate \
  -i http://localhost:8000/openapi.json \
  -g typescript-axios \
  -o ./client
```

---

## 📖 Full Guide

For detailed documentation, see: **[docs/SWAGGER_UI_GUIDE.md](docs/SWAGGER_UI_GUIDE.md)**

---

## ✨ That's It!

Your API is fully documented at: **http://localhost:8000/docs**

Happy coding! 🚀

