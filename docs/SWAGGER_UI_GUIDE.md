# 📖 Swagger UI Guide - API Documentation

## 🎉 Your API Documentation is Live!

FastAPI automatically generates beautiful, interactive API documentation using Swagger UI and ReDoc.

---

## 📍 Access Your API Documentation

### 🌐 Interactive Swagger UI (Recommended)

**URL:** **http://localhost:8001/docs**

**Features:**
- ✅ **Try APIs instantly** - Execute requests directly in your browser
- ✅ **See live examples** - Real request/response samples
- ✅ **Explore all endpoints** - Browse 50+ endpoints across 10 data sources
- ✅ **View schemas** - Detailed request/response structures
- ✅ **Test authentication** - Try with your API keys
- ✅ **Copy curl commands** - Export working examples
- ✅ **Download spec** - Get OpenAPI JSON schema

### 📄 Alternative ReDoc UI

**URL:** **http://localhost:8001/redoc**

**Features:**
- ✅ Clean, read-only documentation
- ✅ Better for reading and printing
- ✅ Three-column layout
- ✅ Search functionality
- ✅ Responsive design

### 💾 OpenAPI Schema (JSON)

**URL:** **http://localhost:8001/openapi.json**

**Use for:**
- ✅ Generating client libraries (Python, TypeScript, Java, etc.)
- ✅ Importing into Postman or Insomnia
- ✅ API testing automation
- ✅ Documentation generation tools

---

## 🚀 How to Use Swagger UI

### Step 1: Start the Service

```bash
# Make sure the service is running
python scripts/start_service.py

# Or manually
docker-compose up -d db
uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
```

### Step 2: Open Swagger UI

Open your browser to: **http://localhost:8001/docs**

You'll see a beautiful interface with all your API endpoints!

### Step 3: Explore Data Sources

The API is organized by data source tags:

#### 📊 Core Endpoints
- **Root** - Service information and health checks
- **jobs** - Track and manage ingestion jobs

#### 📈 Data Sources
- **📊 census** - U.S. Census Bureau demographics
- **💰 fred** - Federal Reserve economic data (800K+ series)
- **⚡ eia** - Energy Information Administration data
- **🏛️ sec** - SEC filings and Form ADV data
- **🌦️ noaa** - NOAA weather and climate data
- **🏠 realestate** - Zillow home values and rental data
- **🗺️ geojson** - Geographic boundaries
- **💼 family_offices** - Family office tracking

### Step 4: Try Your First API Call

#### Example: Start a Census Data Ingestion

1. **Find the endpoint:**
   - Scroll to the **"jobs"** section
   - Click on `POST /api/v1/jobs`

2. **Try it out:**
   - Click the **"Try it out"** button
   - Edit the request body:
   ```json
   {
     "source": "census",
     "config": {
       "survey": "acs5",
       "year": 2023,
       "table_id": "B01001",
       "geo_level": "state"
     }
   }
   ```
   - Click **"Execute"**

3. **View the response:**
   - See the JSON response with your `job_id`
   - Copy the curl command
   - Check response status code (should be 200)

4. **Track the job:**
   - Copy the `job_id` from the response
   - Go to `GET /api/v1/jobs/{job_id}`
   - Click "Try it out"
   - Paste the job ID
   - Click "Execute" to see job status

#### Example: Query Economic Data (FRED)

1. **Navigate to FRED section:**
   - Find **"fred"** tag
   - Click on `GET /api/v1/fred/search`

2. **Search for series:**
   - Click "Try it out"
   - Set parameters:
     - `query`: "unemployment rate"
     - `limit`: 10
   - Click "Execute"

3. **View results:**
   - See matching economic series
   - Pick a `series_id` (e.g., "UNRATE")

4. **Ingest the data:**
   - Go to `POST /api/v1/jobs`
   - Use this body:
   ```json
   {
     "source": "fred",
     "config": {
       "series_id": "UNRATE",
       "start_date": "2020-01-01",
       "end_date": "2024-12-31"
     }
   }
   ```

---

## 📊 Understanding the Swagger Interface

### Endpoint Details

Each endpoint shows:

#### 1. **HTTP Method & Path**
```
POST /api/v1/jobs
```

#### 2. **Description**
What the endpoint does and when to use it

#### 3. **Parameters**
- **Path parameters** (in the URL)
- **Query parameters** (after `?` in URL)
- **Request body** (JSON payload)

#### 4. **Request Body Schema**
Shows the structure of data you need to send:
```json
{
  "source": "string",
  "config": {
    "additionalProp1": "string"
  }
}
```

#### 5. **Responses**
- **200 OK** - Success response
- **422 Validation Error** - Invalid request
- **500 Server Error** - Server issues

#### 6. **Example Values**
Pre-filled examples you can use directly

---

## 💡 Pro Tips & Advanced Usage

### 1. Download OpenAPI Specification

```bash
# Download the spec file
curl http://localhost:8001/openapi.json > api-spec.json

# Or click "Download" in Swagger UI (top right)
```

### 2. Import into Postman

1. Open Postman
2. Click **"Import"**
3. Select **"Link"**
4. Paste: `http://localhost:8001/openapi.json`
5. Click **"Import"**

Now you have all endpoints in Postman!

### 3. Generate Client Libraries

Use the OpenAPI spec to auto-generate client code:

#### Python Client
```bash
# Install generator
pip install openapi-python-client

# Generate client
openapi-python-client generate --url http://localhost:8001/openapi.json

# Use the client
from nexdata_client import Client
from nexdata_client.models import JobCreate

client = Client(base_url="http://localhost:8001")
```

#### TypeScript/JavaScript Client
```bash
# Install generator
npm install -g @openapitools/openapi-generator-cli

# Generate TypeScript client
openapi-generator-cli generate \
  -i http://localhost:8001/openapi.json \
  -g typescript-axios \
  -o ./typescript-client

# Use in your app
import { DefaultApi } from './typescript-client';

const api = new DefaultApi();
```

### 4. Test with Authentication (Future)

If authentication is added:

1. Click **"Authorize"** button (top right)
2. Enter your API key or token
3. Click **"Authorize"**
4. All subsequent requests will include auth

### 5. Share Documentation

**Local sharing:**
```
http://localhost:8001/docs
```

**Remote sharing (if deployed):**
```
https://your-domain.com/docs
```

### 6. Search Endpoints

Use your browser's search (Ctrl+F / Cmd+F) to find:
- Endpoint paths
- Parameter names
- Response fields
- Descriptions

---

## 🎨 What's Included in the Documentation

### Comprehensive Coverage

The Swagger UI documents **all 50+ endpoints** including:

#### Job Management (Core)
- `POST /api/v1/jobs` - Start ingestion
- `GET /api/v1/jobs/{job_id}` - Check status
- `GET /api/v1/jobs` - List all jobs

#### Census Bureau
- `POST /api/v1/census/batch-ingest` - Batch ingestion
- `GET /api/v1/census/metadata/datasets` - Available datasets
- `GET /api/v1/census/metadata/variables` - Table variables
- `GET /api/v1/census/geographies` - Geographic levels

#### Federal Reserve (FRED)
- `POST /api/v1/fred/ingest` - Ingest time series
- `GET /api/v1/fred/search` - Search series
- `GET /api/v1/fred/series/{series_id}` - Series metadata
- `GET /api/v1/fred/categories` - Browse categories

#### Energy (EIA)
- `POST /api/v1/eia/ingest` - Ingest energy data
- `GET /api/v1/eia/series` - List series

#### SEC
- `POST /api/v1/sec/form-adv/ingest/family-offices` - Ingest Form ADV
- `GET /api/v1/sec/form-adv/firms` - Query firms
- `GET /api/v1/sec/form-adv/firms/{crd_number}` - Firm details
- `GET /api/v1/sec/companies/{cik}` - Company facts

#### NOAA Weather
- `POST /api/v1/noaa/ingest` - Ingest weather data
- `GET /api/v1/noaa/datasets` - Available datasets
- `GET /api/v1/noaa/stations` - Weather stations

#### Real Estate
- `POST /api/v1/realestate/ingest` - Ingest Zillow data
- `GET /api/v1/realestate/series` - Available series

#### GeoJSON
- `GET /api/v1/geojson/state/{state_code}` - State boundaries
- `GET /api/v1/geojson/county/{state_code}/{county_code}` - County boundaries

---

## 🔧 Customization

### Modify API Metadata

Edit `app/main.py`:

```python
app = FastAPI(
    title="Your Custom Title",
    description="Your custom description with **markdown**",
    version="1.0.0",
    contact={
        "name": "Your Name",
        "url": "https://your-site.com",
        "email": "your@email.com"
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    },
    terms_of_service="https://your-site.com/terms",
    docs_url="/docs",  # Swagger UI path
    redoc_url="/redoc",  # ReDoc path
    openapi_url="/openapi.json"  # Schema path
)
```

### Add Endpoint Tags

In your router files:

```python
router = APIRouter(
    prefix="/custom",
    tags=["🎯 Custom Tag"],
)

@router.get("/endpoint", tags=["custom"])
def my_endpoint():
    """
    This description appears in Swagger.
    
    - Use **markdown** for formatting
    - Add bullet points
    - Include code examples
    """
    pass
```

### Add Custom Description

```python
@router.post("/jobs", 
    summary="Start an ingestion job",
    description="""
    Start a new data ingestion job from any supported source.
    
    ## Supported Sources
    - census
    - fred
    - eia
    - sec
    - noaa
    - realestate
    
    ## Returns
    A job_id to track progress.
    """,
    response_description="Job created successfully"
)
def create_job(job: JobCreate):
    pass
```

---

## 🚨 Troubleshooting

### Swagger UI Not Loading

**Problem**: `/docs` returns 404 or blank page

**Solutions:**
```bash
# Check if service is running
curl http://localhost:8001/health

# Restart the service
python scripts/start_service.py

# Check for port conflicts
netstat -ano | findstr :8001  # Windows
lsof -i :8001  # Mac/Linux
```

### "Failed to fetch" Error

**Problem**: Swagger can't reach API endpoints

**Solutions:**
1. Check console for CORS errors
2. Verify API is actually running
3. Test endpoint directly:
   ```bash
   curl http://localhost:8001/
   ```

### Endpoints Not Showing

**Problem**: Some endpoints missing from Swagger

**Solutions:**
1. Check router is included in `main.py`:
   ```python
   app.include_router(my_router, prefix="/api/v1")
   ```
2. Restart server
3. Clear browser cache

### OpenAPI Schema Errors

**Problem**: Pydantic validation errors in Swagger

**Solution**: Check your schema definitions in `app/core/schemas.py` match your actual data models.

---

## 📱 Access from Other Devices

### Local Network Access

1. **Find your machine's IP:**
   ```bash
   # Windows
   ipconfig
   
   # Mac/Linux
   ifconfig
   ```

2. **Access from other devices:**
   ```
   http://YOUR_LOCAL_IP:8001/docs
   ```

3. **Update CORS if needed** in `app/main.py`:
   ```python
   app.add_middleware(
       CORSMiddleware,
       allow_origins=["*"],  # Or specific origins
       allow_credentials=True,
       allow_methods=["*"],
       allow_headers=["*"],
   )
   ```

---

## ✅ Quick Reference

### URLs to Remember

| Resource | URL | Purpose |
|----------|-----|---------|
| **Swagger UI** | http://localhost:8001/docs | Interactive API docs |
| **ReDoc** | http://localhost:8001/redoc | Alternative docs |
| **OpenAPI Schema** | http://localhost:8001/openapi.json | Machine-readable spec |
| **Root** | http://localhost:8001/ | Service info |
| **Health Check** | http://localhost:8001/health | Status check |

### Common Tasks

| Task | Steps |
|------|-------|
| View all endpoints | Open `/docs` |
| Test an endpoint | Click endpoint → "Try it out" → Execute |
| Copy curl command | Execute request → Copy curl |
| Download schema | Open `/docs` → Download button |
| Import to Postman | Import → Link → `/openapi.json` |
| Search endpoints | Ctrl+F in `/docs` |

---

## 🎓 Learning Resources

### FastAPI Documentation
- [FastAPI Official Docs](https://fastapi.tiangolo.com/)
- [OpenAPI Specification](https://swagger.io/specification/)
- [Swagger UI Guide](https://swagger.io/tools/swagger-ui/)

### Video Tutorials
Search YouTube for:
- "FastAPI Swagger tutorial"
- "OpenAPI specification guide"
- "Postman OpenAPI import"

---

## ✨ Summary

**You now have:**
- ✅ **Interactive API documentation** at `/docs`
- ✅ **Alternative docs** at `/redoc`
- ✅ **OpenAPI schema** at `/openapi.json`
- ✅ **50+ documented endpoints** across 10 data sources
- ✅ **Try-it-out functionality** for instant testing
- ✅ **Request/response examples** for every endpoint
- ✅ **Copy-paste curl commands**
- ✅ **Schema definitions** for all models

**Next steps:**
1. 🌐 Open **http://localhost:8001/docs**
2. 🔍 Explore your data sources
3. 🚀 Test your first API call
4. 📤 Share with your team!

---

🎉 **Your API is fully documented and ready to use!**

**Need help?** Check the main README or open an issue on GitHub.
