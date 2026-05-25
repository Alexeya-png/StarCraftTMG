## TMG ELO

A web application for tracking matches and displaying an ELO rating system for StarCraft TMG.

About the Project

TMG ELO is a website for:

- submitting match results
- viewing the global player leaderboard
- viewing the list of played matches
- viewing player profiles
- administratively editing players and matches

The project is designed for local use first, with later deployment to a server.

## Main Features
- ELO rating for 1v1 matches
- automatic player creation on first match submission
- race selection for both players
- automatic determination of a player’s primary race based on the number of matches played
- leaderboard table
- game reports list
- player profile with statistics
- admin mode
- player editing
- match editing and deletion
- rating recalculation after match changes

## Supabase cache webhook

The app exposes `POST /api/supabase/cache-webhook` to refresh the in-memory, page, league and disk caches after direct Supabase changes.

Server env required:

- `SUPABASE_WEBHOOK_SECRET` - shared secret used by the webhook request.
- `APP_CACHE_REFRESH_BACKGROUND=0` - refresh synchronously so the next page/API request sees fresh data. Set to `1` only if the webhook starts timing out.
- `APP_HEALTH_CHECK_DB=1` - make `/health` verify Supabase access instead of only checking that Flask is running.

Recommended cache env:

- `APP_BLOCKING_CACHE_LOAD_ON_MISS=1` - load live data synchronously on a cold process instead of serving an empty first response.
- `APP_ALLOW_EMPTY_CACHE_ON_MISS=0` - keep cold processes from returning empty pages while a background refresh is still running.
- `APP_USE_DISK_CACHE_ON_MISS=0` - do not use `app/.cache/application_data.json` as a startup data source unless you explicitly want an offline fallback.
- `APP_DISPLAY_VERSION=v2` - optional label shown in the site header; use it to confirm every page is running the same deployed build.

Supabase setup:

1. Set the same secret in the app env and in the webhook Authorization header as `Bearer <secret>`.
2. Run `Tools/supabase_cache_webhook.sql` in Supabase SQL editor after replacing `REPLACE_WITH_SUPABASE_WEBHOOK_SECRET`.
3. Test with `POST https://tmg-stats.org/api/supabase/cache-webhook` and the same Authorization header.

## Firebase app auth

The site can authenticate against Firebase Auth and read roles from Firestore `users/{firebase_uid}` documents.
When Firebase env is complete, `/login` expects email/password and creates a normal site session for users whose Firestore `role` is in `FIREBASE_LOGIN_ROLES`.
Admin tools still require a role from `ADMIN_ALLOWED_ROLES`.
The login page uses Firebase Auth in the browser and then sends the Firebase ID token to Flask, where the role is checked again before the session cookie is created.
If Firebase env is not set at all, the legacy `ADMIN_USERNAME` / `ADMIN_PASSWORD` login remains active for local use.

Required server env:

```dotenv
FIREBASE_WEB_API_KEY=your_web_api_key
FIREBASE_PROJECT_ID=your_project_id
FIREBASE_DATABASE_ID=starcrafttmgbeta
FIREBASE_LOGIN_ROLES=user,operator,aggregator,admin
ADMIN_ALLOWED_ROLES=admin
FIREBASE_OPERATOR_EMAILS=operator@example.com
```

Optional env:

```dotenv
FIREBASE_EMAIL_VERIFICATION_CUTOFF=2026-03-10T00:00:00Z
FIREBASE_AUTH_TIMEOUT_SECONDS=8
```

Expected Firestore user fields:

- `role`: `user` or `operator` for a normal operator session; `admin` for admin-panel access. Add extra admin roles with `ADMIN_ALLOWED_ROLES=admin,aggregator`.
- `nickname`: optional display name shown in the site header.

If Firestore rules block server-side reads of `users/{uid}`, keep operator emails in `FIREBASE_OPERATOR_EMAILS` until a service account or server-readable role rule is configured.

## Deployment archive

Build the hosting archive with:

```powershell
.\Tools\build_deploy_zip.ps1
```

If Windows blocks local scripts, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\Tools\build_deploy_zip.ps1
```

Do not package `app/.cache` or `__pycache__`; those files can make hosting serve stale data from an old snapshot.

<img width="1890" height="899" alt="image" src="https://github.com/user-attachments/assets/78898ff6-da24-4a75-ba3b-ca46fd9c3080" />
<img width="1910" height="905" alt="image" src="https://github.com/user-attachments/assets/17677d81-82aa-4501-bbc7-b10a391eb32b" />
<img width="1798" height="819" alt="image" src="https://github.com/user-attachments/assets/31e56c0e-ccd9-4d3b-b051-bfc93045dde5" />

