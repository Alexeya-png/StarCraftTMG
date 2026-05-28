import { initializeApp, getApp, getApps } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js';
import { getFirestore, collection, getDocs, doc, getDoc, query, where } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js';

const firebaseConfig = {
    apiKey: 'AIzaSyDHRhS4FIO_1s_2Tn2C77noJRgbs-y_mks',
    authDomain: 'starcrafttmgbeta.firebaseapp.com',
    projectId: 'starcrafttmgbeta',
    storageBucket: 'starcrafttmgbeta.firebasestorage.app',
    messagingSenderId: '70752777650',
    appId: '1:70752777650:web:46b8849504c846f260a0ab',
    measurementId: 'G-QDY06Q6F95'
};

const firebaseApp = getApps().length ? getApp() : initializeApp(firebaseConfig);
const firebaseDb = getFirestore(firebaseApp, 'starcrafttmgbeta');

const rosterPdfCache = {
    tacticalCards: null,
    armyUnits: null,
    gameCards: null,
    activeSeed: null,
};

function normalizeSeed(value) {
    return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '').slice(0, 80);
}

function parseResource(value) {
    if (value === null || value === undefined) return 0;
    const numeric = Number.parseFloat(String(value).trim().replace(',', '.'));
    return Number.isFinite(numeric) ? numeric : 0;
}

function clonePlain(value) {
    if (typeof structuredClone === 'function') {
        return structuredClone(value || {});
    }
    return JSON.parse(JSON.stringify(value || {}));
}

function setDownloadingState(seed, isDownloading, buttonText) {
    const cleanSeed = normalizeSeed(seed);
    const nodes = document.querySelectorAll('[data-roster-download]');
    nodes.forEach(function (node) {
        if (normalizeSeed(node.getAttribute('data-roster-download')) !== cleanSeed) {
            return;
        }
        if (isDownloading) {
            node.dataset.downloadOriginalText = node.textContent;
            node.textContent = buttonText || 'PDF...';
            node.style.pointerEvents = 'none';
            node.style.opacity = '0.7';
        } else {
            if (node.dataset.downloadOriginalText) {
                node.textContent = node.dataset.downloadOriginalText;
                delete node.dataset.downloadOriginalText;
            }
            node.style.pointerEvents = '';
            node.style.opacity = '';
        }
    });
}

function loadScript(src) {
    return new Promise(function (resolve, reject) {
        const existing = document.querySelector('script[src="' + src + '"]');
        if (existing) {
            if (window.html2pdf) {
                resolve();
                return;
            }
            existing.addEventListener('load', function () { resolve(); }, { once: true });
            existing.addEventListener('error', function () { reject(new Error('Failed to load script.')); }, { once: true });
            return;
        }

        const script = document.createElement('script');
        script.src = src;
        script.onload = function () { resolve(); };
        script.onerror = function () { reject(new Error('Failed to load script: ' + src)); };
        document.head.appendChild(script);
    });
}

async function ensureHtml2Pdf() {
    if (window.html2pdf) {
        return;
    }
    await loadScript('https://cdnjs.cloudflare.com/ajax/libs/html2pdf.js/0.10.1/html2pdf.bundle.min.js');
    if (!window.html2pdf) {
        throw new Error('html2pdf did not initialize.');
    }
}

async function loadTacticalCards() {
    if (rosterPdfCache.tacticalCards) {
        return rosterPdfCache.tacticalCards;
    }
    const snapshot = await getDocs(collection(firebaseDb, 'tactical_cards'));
    rosterPdfCache.tacticalCards = snapshot.docs.map(function (item) {
        return { id: item.id, ...item.data() };
    });
    return rosterPdfCache.tacticalCards;
}

async function loadArmyUnits() {
    if (rosterPdfCache.armyUnits) {
        return rosterPdfCache.armyUnits;
    }
    const snapshot = await getDocs(collection(firebaseDb, 'army_units'));
    rosterPdfCache.armyUnits = snapshot.docs.map(function (item) {
        return { id: item.id, ...item.data() };
    });
    return rosterPdfCache.armyUnits;
}

async function loadGameCards() {
    if (rosterPdfCache.gameCards) {
        return rosterPdfCache.gameCards;
    }
    const snapshot = await getDocs(query(collection(firebaseDb, 'faction_cards'), where('faction', '==', 'the_game')));
    rosterPdfCache.gameCards = snapshot.docs.map(function (item) {
        return { id: item.id, ...item.data() };
    });
    return rosterPdfCache.gameCards;
}

async function fetchSharedRoster(seed) {
    const snapshot = await getDoc(doc(firebaseDb, 'shared_rosters', seed));
    if (!snapshot.exists()) {
        throw new Error('Roster ' + seed + ' not found in shared_rosters.');
    }
    const data = snapshot.data();
    if (!data || !data.state) {
        throw new Error('Roster ' + seed + ' does not contain state.');
    }
    return data.state;
}

function generatePrintLayout(state, dbUnits, dbCards, dbGameCards, currentSeed) {
    const factionName = String(state.faction || 'Unknown').toUpperCase();
    const totalMin = Number(state.mineralsUsed || 0);
    const limitMin = Number(state.mineralsLimit || 0);
    const totalGas = Number(state.gasUsed || 0);
    const totalSupply = Number(state.supplyUsed || 0);
    const safeSeed = currentSeed || 'UNSAVED';

    let factionCardName = 'None Selected';
    if (state.factionCardId) {
        const factionCard = dbCards.find((card) => card.id === state.factionCardId);
        if (factionCard) factionCardName = factionCard.name;
    }

    let tacticsHtml = '';
    if (Array.isArray(state.tacticalCardIds) && state.tacticalCardIds.length > 0) {
        tacticsHtml = '<ul style="margin:0; padding-left:20px;">';
        const cardCounts = {};
        state.tacticalCardIds.forEach((id) => {
            cardCounts[id] = (cardCounts[id] || 0) + 1;
        });
        Object.keys(cardCounts).forEach((id) => {
            const card = dbCards.find((item) => item.id === id);
            if (!card) return;
            const isUnique = card.isUnique ? ' (Unique)' : '';
            tacticsHtml += `<li style="margin-bottom:4px;"><b>${cardCounts[id]}x ${card.name}</b>${isUnique} <span style="font-size:10px; color:#666;">(${card.cost} Gas)</span></li>`;
        });
        tacticsHtml += '</ul>';
    } else {
        tacticsHtml = '<span style="color:#999; font-style:italic;">No Tactical Cards selected.</span>';
    }

    const missionNames = Array.isArray(state.missionIds) && state.missionIds.length > 0
        ? state.missionIds.map((id) => {
            const card = dbGameCards.find((item) => item.id === id);
            return card ? card.name : 'Unknown ID';
        }).join(', ')
        : 'None Selected';

    const deployNames = Array.isArray(state.deploymentIds) && state.deploymentIds.length > 0
        ? state.deploymentIds.map((id) => {
            const card = dbGameCards.find((item) => item.id === id);
            return card ? card.name : 'Unknown ID';
        }).join(', ')
        : 'None Selected';

    let unitsHtml = '';
    (state.roster || []).forEach((unit) => {
        const displayUnitName =
            String(unit.name || '').trim().toLowerCase() === 'marine' &&
            String(unit.size || '').trim().toLowerCase() === 'large'
                ? 'Special Forces'
                : (unit.name || 'Unknown Unit');

        let upgradeCost = 0;
        const upgradesList = [];
        (unit.activeUpgrades || []).forEach((index) => {
            const upgrade = (unit.availableUpgrades || [])[index];
            if (!upgrade) return;
            const cost = unit.size === 'small' ? Number(upgrade.costS || 0) : Number(upgrade.costL || 0);
            upgradeCost += cost;
            upgradesList.push(`${upgrade.name} (+${cost})`);
        });

        const totalCost = Number(unit.baseCost || 0) + upgradeCost;
        const dbUnit = dbUnits.find((item) => item.id === unit.id);
        const tagsValue = String(dbUnit?.tags || '').toLowerCase();
        const isUnique = tagsValue.includes('unique');
        const uniqueBadge = isUnique ? '<span style="background:#000; color:#fff; padding:1px 4px; font-size:9px; border-radius:3px; margin-left:5px;">UNIQUE</span>' : '';

        unitsHtml += `
            <tr style="border-bottom:1px solid #ccc;">
                <td style="padding:5px 6px; width:24%; vertical-align:top;">
                    <b>${displayUnitName}</b> ${uniqueBadge}<br>
                    <span style="font-size:9px; color:#555; line-height:1.2;">${String(unit.size || '').toUpperCase()} | Models: ${unit.models ?? '-'} | Type: ${unit.unitType || '-'}</span>
                </td>
                <td style="padding:5px 4px; width:8%; text-align:center; vertical-align:top;">${unit.supply ?? '-'}</td>
                <td style="padding:5px 6px; width:23%; font-size:10px; line-height:1.25; vertical-align:top;">
                    HP: <b>${unit.stats?.hp ?? '-'}</b> | Armor: <b>${unit.stats?.armor ?? '-'}</b> | Shield: <b>${unit.stats?.shield ?? '-'}</b><br>
                    Evade: <b>${unit.stats?.evade ?? '-'}</b> | Speed: <b>${unit.stats?.speed ?? '-'}</b>
                </td>
                <td style="padding:5px 6px; width:33%; font-size:10px; line-height:1.25; vertical-align:top; word-break:break-word;">${upgradesList.length > 0 ? upgradesList.join(', ') : '-'}</td>
                <td style="padding:5px 6px; width:12%; text-align:right; font-weight:bold; vertical-align:top;">${totalCost}</td>
            </tr>
        `;
    });

    const container = document.createElement('div');
    container.innerHTML = `
        <div style="font-family: Arial, sans-serif; padding: 16px; color: #000; background: #fff; max-width: 760px; margin: 0 auto;">
            <div style="border-bottom: 2px solid #000; padding-bottom: 10px; margin-bottom: 20px; display:flex; justify-content:space-between; align-items:flex-end;">
                <div>
                    <h1 style="margin:0; font-size:24px;">STARCRAFT TMG</h1>
                    <div style="font-size:14px; color:#444;">UNOFFICIAL ROSTER SHEET</div>
                </div>
                <div style="text-align:right;">
                    <div style="background:#000; color:#fff; padding:4px 8px; border-radius:4px; margin-bottom:5px; display:inline-block;">
                        SEED: <span style="font-weight:bold; font-family:monospace; font-size:16px;">${safeSeed}</span>
                    </div>
                    <div style="font-size:20px; font-weight:bold;">${factionName}</div>
                    <div style="font-size:12px;">${new Date().toLocaleDateString()}</div>
                </div>
            </div>

            <div style="display:flex; gap:20px; margin-bottom:20px; background:#f3f4f6; padding:15px; border-radius:8px;">
                <div style="flex:1; text-align:center;">
                    <div style="font-size:10px; text-transform:uppercase; color:#666;">Minerals</div>
                    <div style="font-size:18px; font-weight:bold;">${totalMin} / ${limitMin}</div>
                </div>
                <div style="flex:1; text-align:center; border-left:1px solid #ccc;">
                    <div style="font-size:10px; text-transform:uppercase; color:#666;">Gas</div>
                    <div style="font-size:18px; font-weight:bold;">${totalGas}</div>
                </div>
                <div style="flex:1; text-align:center; border-left:1px solid #ccc;">
                    <div style="font-size:10px; text-transform:uppercase; color:#666;">Supply</div>
                    <div style="font-size:18px; font-weight:bold;">${totalSupply}</div>
                </div>
            </div>

            <div style="margin-bottom:20px; display:grid; grid-template-columns: 1fr 1fr; gap:20px;">
                <div>
                    <h3 style="border-bottom:1px solid #000; font-size:14px; margin-bottom:10px;">COMMAND CARDS</h3>
                    <div style="margin-bottom:10px;">
                        <div style="font-size:11px; font-weight:bold; margin-bottom:4px; color:#444;">FACTION CARD</div>
                        <div style="background:#eee; padding:8px; border-radius:4px; font-weight:bold;">${factionCardName}</div>
                    </div>
                    <div>
                        <div style="font-size:11px; font-weight:bold; margin-bottom:4px; color:#444;">TACTICAL CARDS</div>
                        ${tacticsHtml}
                    </div>
                </div>

                <div>
                    <h3 style="border-bottom:1px solid #000; font-size:14px; margin-bottom:10px;">THE GAME</h3>
                    <div style="margin-bottom:10px;">
                        <div style="font-size:11px; font-weight:bold; margin-bottom:4px; color:#444;">SELECTED MISSIONS</div>
                        <div style="background:#eee; padding:8px; border-radius:4px; min-height:30px;">${missionNames}</div>
                    </div>
                    <div>
                        <div style="font-size:11px; font-weight:bold; margin-bottom:4px; color:#444;">SELECTED DEPLOYMENTS</div>
                        <div style="background:#eee; padding:8px; border-radius:4px; min-height:30px;">${deployNames}</div>
                    </div>
                </div>
            </div>

            <h3 style="border-bottom:1px solid #000; font-size:14px; margin-bottom:10px;">UNIT ROSTER</h3>
            <table style="width:100%; border-collapse: collapse; table-layout:fixed; font-size:11px;">
                <thead>
                    <tr style="background:#000; color:#fff;">
                        <th style="padding:6px; width:24%; text-align:left;">UNIT</th>
                        <th style="padding:6px; width:8%; text-align:center;">SUPPLY</th>
                        <th style="padding:6px; width:23%; text-align:left;">STATS</th>
                        <th style="padding:6px; width:33%; text-align:left;">UPGRADES</th>
                        <th style="padding:6px; width:12%; text-align:right;">COST</th>
                    </tr>
                </thead>
                <tbody>
                    ${unitsHtml || '<tr><td colspan="5" style="padding:14px; text-align:center;">No units mustered.</td></tr>'}
                </tbody>
            </table>

            <div style="margin-top:30px; text-align:center; font-size:10px; color:#888; border-top:1px solid #eee; padding-top:10px;">
                Generated by tmg-stats | Share SEED: <b>${safeSeed}</b>
            </div>
        </div>
    `;
    return container;
}

function recalculateState(stateValue, dbCards) {
    const nextState = clonePlain(stateValue);
    nextState.missionIds = Array.isArray(nextState.missionIds) ? nextState.missionIds : [];
    nextState.deploymentIds = Array.isArray(nextState.deploymentIds) ? nextState.deploymentIds : [];
    nextState.roster = Array.isArray(nextState.roster) ? nextState.roster : [];
    nextState.tacticalCardIds = Array.isArray(nextState.tacticalCardIds) ? nextState.tacticalCardIds : [];

    nextState.gasLimit = Math.floor(Number(nextState.mineralsLimit || 0) * 0.10);

    let gasUsed = 0;
    nextState.tacticalCardIds.forEach(function (id) {
        const card = dbCards.find(function (item) { return item.id === id; });
        if (card) {
            gasUsed += Number(card.cost || 0);
        }
    });
    nextState.gasUsed = gasUsed;

    let resourceTotal = 0;
    const addResource = function (cardId) {
        const card = dbCards.find(function (item) { return item.id === cardId; });
        if (!card) {
            return;
        }
        if (card.resource !== undefined && card.resource !== null && String(card.resource).trim() !== '') {
            resourceTotal += parseResource(card.resource);
        }
    };

    if (nextState.factionCardId) {
        addResource(nextState.factionCardId);
    }
    nextState.tacticalCardIds.forEach(function (id) {
        addResource(id);
    });
    nextState.resourceTotal = resourceTotal;

    let mineralsUsed = 0;
    let supplyUsed = 0;
    nextState.roster.forEach(function (unit) {
        let upgradeCost = 0;
        (unit.activeUpgrades || []).forEach(function (index) {
            const upgrade = (unit.availableUpgrades || [])[index];
            if (!upgrade) {
                return;
            }
            upgradeCost += unit.size === 'small' ? Number(upgrade.costS || 0) : Number(upgrade.costL || 0);
        });
        mineralsUsed += Number(unit.baseCost || 0) + upgradeCost;
        supplyUsed += Number(unit.supply || 0);
    });

    nextState.mineralsUsed = mineralsUsed;
    nextState.supplyUsed = supplyUsed;
    return nextState;
}

async function downloadRosterPdfBySeed(seed) {
    const cleanSeed = normalizeSeed(seed);
    if (!cleanSeed) {
        throw new Error('Invalid roster seed.');
    }

    rosterPdfCache.activeSeed = cleanSeed;
    setDownloadingState(cleanSeed, true, 'PDF...');

    try {
        const rawState = await fetchSharedRoster(cleanSeed);
        const results = await Promise.all([
            loadTacticalCards(),
            loadArmyUnits(),
            loadGameCards(),
        ]);

        const dbCards = results[0];
        const dbUnits = results[1];
        const dbGameCards = results[2];

        const preparedState = recalculateState(rawState, dbCards);
        await ensureHtml2Pdf();

        const printContent = generatePrintLayout(preparedState, dbUnits, dbCards, dbGameCards, cleanSeed);
        const options = {
            margin: 10,
            filename: 'SC_TMG_' + (preparedState.faction || 'Unknown') + '_Roster_' + cleanSeed + '.pdf',
            image: { type: 'jpeg', quality: 0.98 },
            html2canvas: { scale: 2, useCORS: true, letterRendering: true },
            jsPDF: { unit: 'mm', format: 'a4', orientation: 'portrait' },
        };

        await window.html2pdf().set(options).from(printContent).save();
    } finally {
        setDownloadingState(cleanSeed, false);
        rosterPdfCache.activeSeed = null;
    }
}

document.addEventListener('click', function (event) {
    const rosterDownloadLink = event.target.closest('[data-roster-download]');
    if (!rosterDownloadLink) {
        return;
    }

    event.preventDefault();
    const rosterId = rosterDownloadLink.getAttribute('data-roster-download') || '';
    if (!rosterId || rosterPdfCache.activeSeed) {
        return;
    }

    downloadRosterPdfBySeed(rosterId).catch(function (error) {
        console.error(error);
        alert(error.message || 'Failed to generate PDF.');
    });
});
