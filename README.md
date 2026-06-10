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
- `APP_CACHE_REFRESH_BACKGROUND=1` - coalesce bursts of Supabase webhook calls and refresh without making Supabase wait.
- `APP_HEALTH_CHECK_DB=1` - make `/health` verify Supabase access instead of only checking that Flask is running.

Recommended cache env:

- `APP_BLOCKING_CACHE_LOAD_ON_MISS=1` - load live data synchronously on a cold process instead of serving an empty first response.
- `APP_ALLOW_EMPTY_CACHE_ON_MISS=0` - keep cold processes from returning empty pages while a background refresh is still running.
- `APP_USE_DISK_CACHE_ON_MISS=0` - do not use `app/.cache/application_data.json` as a startup data source unless you explicitly want an offline fallback.
- `APP_SHARED_CACHE_SYNC_INTERVAL_SECONDS=5` - each Namecheap worker checks the shared local snapshot at most once every five seconds.
- `APP_CACHE_REFRESH_AFTER_WRITE_BACKGROUND=0` - finish rebuilding the shared snapshot before a successful write request returns.
- `APP_DISPLAY_VERSION=v2` - optional label shown in the site header; use it to confirm every page is running the same deployed build.

Supabase setup:

1. Set the same secret in the app env and in the webhook Authorization header as `Bearer <secret>`.
2. Run `Tools/supabase_cache_webhook.sql` in Supabase SQL editor after replacing `REPLACE_WITH_SUPABASE_WEBHOOK_SECRET`.
3. Test with `POST https://tmg-stats.org/api/supabase/cache-webhook` and the same Authorization header.

Leaderboard, reports, and league pages request their API again every five seconds while the browser tab is visible. Those requests read the Namecheap cache; they do not download the full database from Supabase every five seconds.
The browser sends the current cache token, so an unchanged response is very small. Requests never overlap, hidden tabs pause polling, and failures progressively back off to one request per minute.

Default application rate limits:

- `APP_LIVE_API_RATE_LIMIT_PER_MINUTE=600` - about 50 polling tabs behind one public IP at the five-second interval.
- `APP_MATCH_SUBMIT_RATE_LIMIT_PER_HOUR=20`
- `APP_TTS_SUBMIT_RATE_LIMIT_PER_HOUR=120`
- `APP_FEEDBACK_RATE_LIMIT_PER_HOUR=10`
- `APP_SUPPORT_CHECKOUT_RATE_LIMIT_PER_MINUTE=20`
- `APP_WEBHOOK_RATE_LIMIT_PER_MINUTE=120`
- `APP_AUTH_RATE_LIMIT_PER_TEN_MINUTES=30`
- `APP_PROTECTED_WRITE_RATE_LIMIT_PER_FIVE_MINUTES=60`

These in-process limits protect against accidental bursts and basic abuse. Put Cloudflare or another edge WAF in front of the shared hosting for DDoS protection and globally enforced limits.

## Stripe project support

The site exposes `/support` with one-time EUR support options and redirects payment details to Stripe Checkout.

For the complete Stripe, Supabase, cPanel, and test-payment setup in Russian, see `STRIPE_SUPPORT_SETUP_RU.md`.

Required server env:

```dotenv
STRIPE_SECRET_KEY=sk_live_replace_me
STRIPE_WEBHOOK_SECRET=whsec_replace_me
SITE_URL=https://tmg-stats.org
```

Supporter badge setup:

1. Run `Tools/add_supporter_badges.sql` in the Supabase SQL editor.
2. In Stripe Dashboard, add `https://tmg-stats.org/api/stripe/webhook` as a webhook endpoint.
3. Subscribe it to `checkout.session.completed` and `checkout.session.async_payment_succeeded`.
4. Put the endpoint signing secret in `STRIPE_WEBHOOK_SECRET`.

After a paid Checkout session, the selected existing player receives the planet supporter badge. If the donor is signed in with a Google account linked to a player, that linked player is selected automatically.
The server must use `SUPABASE_SERVICE_ROLE_KEY`; the public anon key cannot call the badge-award database function.

Configure payout bank details only in Stripe Dashboard. Never store the Stripe secret key or bank account details in the repository.

## Player accounts and Google login

Run `Tools/add_user_accounts_profile_features.sql` in Supabase before enabling player registration.
It adds Google-backed user accounts, player aliases, nickname color choices, ladder display preferences, and per-race offrace ELO storage.

Required env:

```dotenv
GOOGLE_CLIENT_ID=your_google_oauth_client_id
GOOGLE_CLIENT_SECRET=your_google_oauth_client_secret
GOOGLE_REDIRECT_URI=https://tmg-stats.org/auth/google/callback
```

Add the same redirect URI in Google Cloud Console. After login, users open `/account`, link their Google account to a player, then manage flag, nickname color, public aliases, offrace ELO, and ladder visibility.

## Firebase app auth

The site can authenticate against Firebase Auth and read roles from Firestore `users/{firebase_uid}` documents.
When Firebase env is complete, `/login` expects email/password and creates a normal site session for users whose Firestore `role` is in `FIREBASE_LOGIN_ROLES`.
Admin tools still require a role from `ADMIN_ALLOWED_ROLES`.
The login page uses Firebase Auth in the browser and then sends the Firebase ID token to Flask, where the role is checked again before the session cookie is created.
If Firebase env is not set at all, the legacy `ADMIN_USERNAME` / `ADMIN_PASSWORD` login remains active for local use.

Do not package `app/.cache` or `__pycache__`; those files can make hosting serve stale data from an old snapshot.

<img width="1890" height="899" alt="image" src="https://github.com/user-attachments/assets/78898ff6-da24-4a75-ba3b-ca46fd9c3080" />
<img width="1910" height="905" alt="image" src="https://github.com/user-attachments/assets/17677d81-82aa-4501-bbc7-b10a391eb32b" />
<img width="1798" height="819" alt="image" src="https://github.com/user-attachments/assets/31e56c0e-ccd9-4d3b-b051-bfc93045dde5" />

