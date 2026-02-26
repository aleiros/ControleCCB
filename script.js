import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import { createUserWithEmailAndPassword, getAuth, onAuthStateChanged, sendPasswordResetEmail, signInWithEmailAndPassword, signOut } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import { addDoc, collection, deleteDoc, doc, getDoc, getDocs, getFirestore, onSnapshot, query, serverTimestamp, setDoc, updateDoc, where } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';

const firebaseConfig = {
    apiKey: "AIzaSyCav4z30NXsM1_YzOngZk562BFTo3W5NoI",
    authDomain: "sistema-de-gestao-de-operacoes.firebaseapp.com",
    databaseURL: "https://sistema-de-gestao-de-operacoes-default-rtdb.firebaseio.com",
    projectId: "sistema-de-gestao-de-operacoes",
    storageBucket: "sistema-de-gestao-de-operacoes.firebasestorage.app",
    messagingSenderId: "368007797722",
    appId: "1:368007797722:web:97e9b30b5e7ef24bb33ee3"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);

// ═══ STATE ═══
let CU = null; // current user profile from Firestore
let ops = [], malotes = [], regs = [], notifs = [];
let _detOpId = null, _detMalId = null, _detRegId = null;
let _unsubOps = null, _unsubMal = null, _unsubReg = null, _unsubNotif = null;

// ═══ UTILS ═══
function g(id) { return document.getElementById(id) }
function v(id) { return (g(id)?.value || '').trim() }
function esc(s) { if (!s) return ''; return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;') }
function showErr(el, msg) { el.textContent = msg; el.classList.remove('hidden') }
function setVals(obj) { for (const [k, val] of Object.entries(obj)) { const el = g(k); if (el) el.value = val || '' } }
function fmtD(d) { if (!d) return ''; if (d?.toDate) d = d.toDate().toISOString(); if (typeof d === 'string' && d.includes('T')) d = d.split('T')[0]; if (typeof d === 'string' && d.includes('-')) { const p = d.split('-'); return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : d; } return d; }
function toISO(s) { if (!s) return ''; s = s.trim(); if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s; if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) { const [d, m, y] = s.split('/'); return `${y}-${m}-${d}`; } if (/^\d{2}\/\d{2}\/\d{2}$/.test(s)) { const [d, m, y] = s.split('/'); return `${parseInt(y) < 50 ? '20' : '19'}${y}-${m}-${d}`; } return s; }
let _tt;
function toast(msg, type = 'info') { const t = g('toast'); t.textContent = msg; t.className = 'show ' + (type === 'ok' ? 'ok' : type === 'err' ? 'err' : 'info'); clearTimeout(_tt); _tt = setTimeout(() => t.classList.remove('show'), 3400); }
function paLabel(pa) { return pa === 'UAD' ? 'Gerais Controle' : pa }
function paBadge(pa) { if (pa === 'UAD') return '<span class="b b-pu">Gerais Controle</span>'; return `<span class="b b-tl">${esc(pa)}</span>`; }
function isAdmin() { return CU?.role === 'admin' }

// Expose global helpers
window.g = g; window.v = v; window.esc = esc; window.toast = toast;

// ═══ ACCESS CONTROL UI ═══
function applyAccessUI() {
    // Show/hide admin-only buttons
    document.querySelectorAll('.admin-only').forEach(el => {
        el.classList.toggle('hidden', !isAdmin());
    });
    // Show read-only banner for PA users
    document.querySelectorAll('.admin-hide').forEach(el => {
        el.classList.toggle('hidden', isAdmin());
    });
    // Dashboard nav só para admin
    const niDash = g('ni-dashboard');
    if (niDash) niDash.classList.toggle('hidden', !isAdmin());
    // Show admin nav
    if (isAdmin()) {
        g('sb-admin').classList.remove('hidden');
        const rt = g('role-tag'); rt.textContent = 'Gerais Controle · Admin'; rt.className = 'role-tag role-admin'; rt.classList.remove('hidden');
    } else {
        g('sb-admin').classList.add('hidden');
        const rt = g('role-tag'); rt.textContent = 'PA ' + CU.pa + ' · Leitura'; rt.className = 'role-tag role-pa'; rt.classList.remove('hidden');
    }
}

// ═══ AUTH SCREENS ═══
function sc(id) { document.querySelectorAll('.auth-card').forEach(c => c.classList.add('hidden')); g(id).classList.remove('hidden'); }
window.sc = sc;

function showAuth() { g('loading').classList.add('hidden'); g('auth').classList.remove('hidden'); g('app').classList.add('hidden'); }
function showApp() { g('loading').classList.add('hidden'); g('auth').classList.add('hidden'); g('app').classList.remove('hidden'); }
function showLoading() { g('loading').classList.remove('hidden'); }

// ═══ AUTH STATE ═══
onAuthStateChanged(auth, async (fbUser) => {
    if (!fbUser) {
        g('loading').classList.add('hidden');
        showAuth(); sc('sc-login');
        unsubAll();
        return;
    }
    // Load user profile from Firestore
    try {
        const snap = await getDoc(doc(db, 'users', fbUser.uid));
        if (!snap.exists()) {
            g('loading').classList.add('hidden');
            showAuth(); sc('sc-login');
            return;
        }
        CU = { uid: fbUser.uid, ...snap.data() };
        if (CU.status === 'pending') { g('loading').classList.add('hidden'); showAuth(); sc('sc-pending'); return; }
        if (CU.status === 'reproved') { g('loading').classList.add('hidden'); showAuth(); sc('sc-reproved'); return; }
        // Active user → show app
        g('u-avt').textContent = CU.name[0].toUpperCase();
        g('u-name').textContent = CU.name;
        g('u-pa').textContent = paLabel(CU.pa);
        g('greeting').textContent = 'Olá, ' + CU.name.split(' ')[0] + '!';
        g('dash-sub').textContent = isAdmin() ? 'Visão geral de todos os PAs' : 'Visão do PA ' + CU.pa;
        applyAccessUI();
        showApp();
        subscribeAll();
        // Admin → Dashboard | PA → direto nas Operações
        nav(isAdmin() ? 'dashboard' : 'ops');
        g('loading').classList.add('hidden');
    } catch (e) {
        console.error(e);
        g('loading').classList.add('hidden');
        showAuth(); sc('sc-login');
    }
});

function unsubAll() {
    [_unsubOps, _unsubMal, _unsubReg, _unsubNotif].forEach(u => { try { u && u(); } catch (e) { } });
}

// Ordena array por campo desc no JS (evita índices compostos no Firestore)
function sortDesc(arr, field) {
    return arr.sort((a, b) => {
        const av = a[field]?.toDate ? a[field].toDate().getTime() : (a[field] || 0);
        const bv = b[field]?.toDate ? b[field].toDate().getTime() : (b[field] || 0);
        return bv - av;
    });
}

function subscribeAll() {
    unsubAll();

    // Operações — sem orderBy para evitar índice composto
    const qOps = isAdmin()
        ? collection(db, 'operacoes')
        : query(collection(db, 'operacoes'), where('pa', '==', CU.pa));
    _unsubOps = onSnapshot(qOps, snap => {
        ops = sortDesc(snap.docs.map(d => ({ id: d.id, ...d.data() })), 'createdAt');
        renderOps(); updateDash();
    }, err => { console.error('ops', err); toast('Erro ao carregar operações.', 'err'); });

    // Malotes
    const qMal = isAdmin()
        ? collection(db, 'malotes')
        : query(collection(db, 'malotes'), where('pa', '==', CU.pa));
    _unsubMal = onSnapshot(qMal, snap => {
        malotes = sortDesc(snap.docs.map(d => ({ id: d.id, ...d.data() })), 'createdAt');
        renderMalotes(); updateDash();
    }, err => { console.error('mal', err); toast('Erro ao carregar malotes.', 'err'); });

    // Regularizações
    const qReg = isAdmin()
        ? collection(db, 'regularizacoes')
        : query(collection(db, 'regularizacoes'), where('pa', '==', CU.pa));
    _unsubReg = onSnapshot(qReg, snap => {
        regs = sortDesc(snap.docs.map(d => ({ id: d.id, ...d.data() })), 'createdAt');
        renderRegs(); updateDash();
    }, err => { console.error('reg', err); toast('Erro ao carregar pendências.', 'err'); });

    // Notificações
    const qN = isAdmin()
        ? collection(db, 'notificacoes')
        : query(collection(db, 'notificacoes'), where('pa', '==', CU.pa));
    _unsubNotif = onSnapshot(qN, snap => {
        notifs = sortDesc(snap.docs.map(d => ({ id: d.id, ...d.data() })), 'createdAt');
        updN();
    }, err => console.error('notif', err));

    // Badge usuários pendentes (admin)
    if (isAdmin()) {
        onSnapshot(query(collection(db, 'users'), where('status', '==', 'pending')), snap => {
            const cnt = snap.size;
            const b = g('pending-badge');
            if (cnt > 0) { b.textContent = cnt; b.classList.remove('hidden'); } else b.classList.add('hidden');
        }, err => console.error('pending', err));
    }
}

// ═══ AUTH ACTIONS ═══
window.doLogin = async function () {
    const em = g('l-email').value.trim(), pw = g('l-pass').value;
    const err = g('l-err'); err.classList.add('hidden');
    if (!em || !pw) { showErr(err, 'Preencha e-mail e senha.'); return; }
    showLoading();
    try {
        await signInWithEmailAndPassword(auth, em, pw);
        // onAuthStateChanged will handle the rest
    } catch (e) {
        g('loading').classList.add('hidden');
        showAuth(); sc('sc-login');
        showErr(err, e.code === 'auth/invalid-credential' ? 'E-mail ou senha incorretos.' : 'Erro: ' + e.message);
    }
};

window.doReg = async function () {
    const name = g('r-name').value.trim(), email = g('r-email').value.trim(), pa = g('r-pa').value, pass = g('r-pass').value;
    const err = g('r-err'); err.classList.add('hidden');
    if (!name || !email || !pa || !pass) { showErr(err, 'Preencha todos os campos.'); return; }
    if (pass.length < 6) { showErr(err, 'Mínimo 6 caracteres.'); return; }
    showLoading();
    try {
        const cred = await createUserWithEmailAndPassword(auth, email, pass);
        await setDoc(doc(db, 'users', cred.user.uid), {
            name, email, pa, role: 'user', status: 'pending', createdAt: serverTimestamp()
        });
        await signOut(auth);
        g('loading').classList.add('hidden');
        showAuth(); sc('sc-pending');
        toast('Solicitação enviada! Aguarde aprovação.', 'ok');
    } catch (e) {
        g('loading').classList.add('hidden'); showAuth(); sc('sc-reg');
        showErr(err, e.code === 'auth/email-already-in-use' ? 'E-mail já cadastrado.' : 'Erro: ' + e.message);
    }
};

window.doForgot = async function () {
    const em = g('f-email').value.trim();
    if (!em) { showErr(g('f-err'), 'Digite o e-mail.'); return; }
    try {
        await sendPasswordResetEmail(auth, em);
        g('f-ok').textContent = 'Link de recuperação enviado para ' + em;
        g('f-ok').classList.remove('hidden');
    } catch (e) { showErr(g('f-err'), 'Erro: ' + e.message); }
};

window.doLogout = async function () {
    unsubAll(); CU = null; ops = []; malotes = []; regs = []; notifs = [];
    try { await signOut(auth); } catch (e) { }
};

// ═══ USER MANAGEMENT ═══
window.approveUser = async function (uid) {
    if (!confirm('Aprovar este usuário?')) return;
    await updateDoc(doc(db, 'users', uid), { status: 'active' });
    toast('Usuário aprovado!', 'ok');
};
window.reproveUser = async function (uid) {
    if (!confirm('Reprovar este usuário?')) return;
    await updateDoc(doc(db, 'users', uid), { status: 'reproved' });
    toast('Usuário reprovado.', 'info');
};
window.promoteUser = async function (uid) {
    if (!confirm('Promover a Admin (Gerais Controle)?')) return;
    await updateDoc(doc(db, 'users', uid), { role: 'admin', pa: 'UAD' });
    toast('Promovido a Admin!', 'ok');
};

// ═══ NAV ═══
window.openSB = function () {
    g('sb').classList.add('open');
    g('sb-overlay').classList.add('show');
    document.body.style.overflow = 'hidden';
};
window.closeSB = function () {
    g('sb').classList.remove('open');
    g('sb-overlay').classList.remove('show');
    document.body.style.overflow = '';
};
window.nav = function (p) {
    closeSB();
    document.querySelectorAll('.pg').forEach(x => x.classList.add('hidden'));
    document.querySelectorAll('.ni').forEach(x => x.classList.remove('active'));
    const pg = g('pg-' + p); if (pg) pg.classList.remove('hidden');
    const ni = document.querySelector('[data-p="' + p + '"]'); if (ni) ni.classList.add('active');
    g('tb-title').textContent = { dashboard: 'Dashboard', malote: 'Malote CCB', reg: 'Pend. Regularização', ops: 'Operações CCB', users: 'Usuários' }[p] || p;
    if (p === 'dashboard') updateDash();
    if (p === 'malote') { ppf('m-fpa', malotes); renderMalotes(); }
    if (p === 'reg') { ppf('r-fpa', regs); renderRegs(); }
    if (p === 'ops') { ppf('o-fpa', ops); renderOps(); }
    if (p === 'users') renderUsers();
};
// Navega para Regularização e filtra por contrato
window.navMalFiltrado = function (contrato) {
    nav('malote');
    setTimeout(() => {
        const si = g('m-search');
        if (si) { si.value = contrato; renderMalotes(); }
    }, 150);
};
window.navRegFiltrado = function (contrato) {
    nav('reg');
    setTimeout(() => {
        const si = g('r-search');
        if (si) { si.value = contrato; renderRegs(); }
    }, 150);
};

// ═══ NOTIFICATIONS ═══
function updN() {
    const u = notifs.filter(n => !n.read).length;
    const b = g('nbadge'); if (u > 0) { b.textContent = u; b.classList.remove('hidden'); } else b.classList.add('hidden');
}
window.toggleNotif = function () {
    const p = g('npanel'); p.classList.toggle('hidden');
    if (!p.classList.contains('hidden')) {
        g('nlist').innerHTML = notifs.length ? notifs.map(n => `<div class="nitem"><div>${esc(n.message)}</div><div class="ntime">${fmtD(n.createdAt)}</div></div>`).join('') : '<p class="empty">Sem notificações</p>';
    }
};
async function addN(pa, msg) {
    try { await addDoc(collection(db, 'notificacoes'), { pa, message: msg, read: false, createdAt: serverTimestamp() }); } catch (e) { }
}

// ═══ DASHBOARD ═══
function updateDash() {
    g('k-tot').textContent = ops.length;
    g('k-enc').textContent = ops.filter(o => o.situacao === 'Encerrado').length;
    g('k-ab').textContent = ops.filter(o => o.situacao === 'Aberto').length;
    g('k-pend').textContent = ops.filter(o => o.regularizacao && o.regularizacao.trim()).length;
    g('k-mal').textContent = malotes.length;
    g('k-malp').textContent = malotes.filter(m => m.statusMalote === 'Pendente').length;
    g('k-reg').textContent = regs.length;
    g('k-pas').textContent = new Set([...ops.map(o => o.pa), ...malotes.map(m => m.pa)].filter(Boolean)).size;
    g('r-ops').innerHTML = ops.slice(0, 5).map(o => `<div class="ritem" onclick="nav('ops');setTimeout(()=>showOpDetail('${o.id}'),120)"><div><div class="ri-n">${esc(o.nome)}</div><div class="ri-m">${paLabel(o.pa)} · ${esc(o.produto)} · ${esc(o.contrato)}</div></div>${sB(o.situacao)}</div>`).join('') || '<p class="empty">Nenhuma operação</p>';
    g('r-pend').innerHTML = regs.slice(0, 5).map(r => `<div class="ritem" onclick="nav('reg');setTimeout(()=>showRegDetail('${r.id}'),120)"><div><div class="ri-n">${esc(r.nome)}</div><div class="ri-m">${paLabel(r.pa)} · ${esc(r.contrato)}</div></div>${rB(r.status)}</div>`).join('') || '<p class="empty">Sem pendências</p>';
    updN();
}

// ═══ DETAIL MODALS ═══
window.openM = function (id) { g(id).classList.remove('hidden'); }
window.closeM = function (id) { g(id).classList.add('hidden'); }

window.showOpDetail = function (id) {
    const o = ops.find(x => x.id === id); if (!o) return;
    _detOpId = id;
    // Find linked regularização by contrato
    const linkedRegs = regs.filter(r => r.opId === o.id || (r.contrato === o.contrato && r.pa === o.pa));
    // Link pendência: se tem 1 → abre detalhe; se tem vários → filtra lista; se nenhum → badge OK
    let regHtml = '<span class="b b-gr">OK</span>';
    if (o.regularizacao && o.regularizacao.trim()) {
        if (linkedRegs.length === 1) {
            const r = linkedRegs[0];
            regHtml = `<span class="b b-or" style="margin-right:6px">⚠ ${esc(o.regularizacao)}</span>`
                + `<span class="reg-link" onclick="closeM('d-op');nav('reg');setTimeout(()=>showRegDetail('${r.id}'),120)">Ver pendência ${rB(r.status)}</span>`;
        } else if (linkedRegs.length > 1) {
            regHtml = `<span class="b b-or" style="margin-right:6px">⚠ ${esc(o.regularizacao)}</span>`
                + `<span class="reg-link" onclick="closeM('d-op');navRegFiltrado('${esc(o.contrato)}')">Ver ${linkedRegs.length} pendências →</span>`;
        } else {
            regHtml = `<span class="b b-or">⚠ ${esc(o.regularizacao)}</span>`
                + ` <span style="font-size:11px;color:var(--muted)">(sem pendência vinculada)</span>`;
        }
    }
    // Malote vinculado
    const linkedMal = malotes.filter(m => m.opId === o.id);
    let malHtml = '<span class="b b-gy">Não gerado</span>';
    if (linkedMal.length === 1) {
        const m = linkedMal[0];
        malHtml = `${mB(m.statusMalote)} <span class="reg-link" onclick="closeM('d-op');nav('malote');setTimeout(()=>showMalDetail('${m.id}'),120)">📦 Ver Malote</span>`;
    } else if (linkedMal.length > 1) {
        malHtml = `<span class="reg-link" onclick="closeM('d-op');navMalFiltrado('${esc(o.contrato)}')">Ver ${linkedMal.length} malotes →</span>`;
    }

    g('d-op-body').innerHTML = detGrid([
        ['Ordem', '#' + esc(String(o.ordem || ''))],
        ['PA', paBadge(o.pa)],
        ['Produto', pB(o.produto)],
        ['Tipo Assinatura', esc(o.tipoAssinatura || '—')],
        ['CPF/CNPJ', esc(o.cpf || '—')],
        ['Nº Cliente', esc(o.clienteNum || '—')],
        ['Nome', '<strong>' + esc(o.nome) + '</strong>'],
        ['Nº Contrato', esc(o.contrato || '—')],
        ['Finalidade', esc(o.finalidade || '—')],
        ['Data Operação', fmtD(o.dataOp) || '—'],
        ['Data Vencimento', fmtD(o.dataVenc) || '—'],
        ['Resolução', fmtD(o.resolucao) || '—'],
        ['Situação', sB(o.situacao)],
        ['Malote', malHtml],
        ['Regularização', regHtml, true],
    ]);
    g('d-op-btns').innerHTML = isAdmin() ?
        `<button class="btn btn-icon del" onclick="delOpFD()">Excluir</button><button class="btn btn-s" onclick="editOpFD()">✏ Editar</button>` :
        `<span style="font-size:12px;color:var(--muted)">Somente leitura</span>`;
    openM('d-op');
};

window.showMalDetail = function (id) {
    const m = malotes.find(x => x.id === id); if (!m) return;
    _detMalId = id;
    const linkedOpMal = ops.find(o => o.id === m.opId);
    const opLinkMal = linkedOpMal ? `<span class="reg-link" onclick="closeM('d-mal');nav('ops');setTimeout(()=>showOpDetail('${linkedOpMal.id}'),120)">Ver Operação ${sB(linkedOpMal.situacao)}</span>` : '—';
    g('d-mal-body').innerHTML = detGrid([
        ['PA', paBadge(m.pa)],
        ['Produto', pB(m.produto)],
        ['Tipo Assinatura', esc(m.tipoAssinatura || '—')],
        ['CPF/CNPJ', esc(m.cpf || '—')],
        ['Nº Cliente', esc(m.clienteNum || '—')],
        ['Nome', '<strong>' + esc(m.nome) + '</strong>'],
        ['Nº Contrato', esc(m.contrato || '—')],
        ['Finalidade', esc(m.finalidade || '—')],
        ['Data Operação', fmtD(m.dataOp) || '—'],
        ['Data Vencimento', fmtD(m.dataVenc) || '—'],
        ['Situação', sB(m.situacao)],
        ['Status Malote', mB(m.statusMalote)],
        ['Doc. Faltando', m.docFaltando ? `<span class="b b-or">⚠ ${esc(m.docFaltando)}</span>` : '<span class="b b-gr">Nenhum</span>', true],
        ['Operação Vinculada', opLinkMal],
    ]);
    g('d-mal-btns').innerHTML = isAdmin() ?
        `<button class="btn btn-icon del" onclick="delMalFD()">Excluir</button><button class="btn btn-s" onclick="editMalFD()">✏ Editar</button>` :
        `<span style="font-size:12px;color:var(--muted)">Somente leitura</span>`;
    openM('d-mal');
};

window.showRegDetail = function (id) {
    const r = regs.find(x => x.id === id); if (!r) return;
    _detRegId = id;
    // Find linked op
    const linkedOp = ops.find(o => o.id === r.opId) || (ops.find(o => o.contrato === r.contrato && o.pa === r.pa));
    let opLink = '—';
    if (linkedOp) opLink = `<span class="reg-link" onclick="closeM('d-reg');nav('ops');setTimeout(()=>showOpDetail('${linkedOp.id}'),120)">Ver Operação ${sB(linkedOp.situacao)}</span>`;
    g('d-reg-body').innerHTML = detGrid([
        ['PA', paBadge(r.pa)],
        ['Produto', pB(r.produto)],
        ['CPF/CNPJ', esc(r.cpf || '—')],
        ['Nome', '<strong>' + esc(r.nome) + '</strong>'],
        ['Nº Contrato', esc(r.contrato || '—')],
        ['Data Abertura', fmtD(r.createdAt) || '—'],
        ['Data Resolução', fmtD(r.resolucao) || '—'],
        ['Status', rB(r.status)],
        ['Operação Vinculada', opLink],
        ['Pendência / Documento', esc(r.pendencia || '—'), true],
    ]);
    g('d-reg-btns').innerHTML = isAdmin() ?
        `<button class="btn btn-icon del" onclick="delRegFD()">Excluir</button><button class="btn btn-s" onclick="editRegFD()">✏ Editar</button>` :
        `<span style="font-size:12px;color:var(--muted)">Somente leitura</span>`;
    openM('d-reg');
};

function detGrid(rows) {
    let h = '<div class="det-grid">';
    for (const [label, val, full] of rows) {
        if (full) { h += `<div style="grid-column:1/-1;display:contents"><div class="det-label" style="grid-column:1/-1;border-right:none">${label}</div><div class="det-val" style="grid-column:1/-1">${val}</div></div>`; }
        else { h += `<div class="det-label">${label}</div><div class="det-val">${val}</div>`; }
    }
    return h + '</div>';
}

// From detail → edit/delete (admin only)
window.editOpFD = function () { const id = _detOpId; closeM('d-op'); editOp(id); }
window.delOpFD = async function () {
    if (!confirm('Excluir operação e registros vinculados (malote + pendência)?')) return;
    closeM('d-op');
    const id = _detOpId;
    // Exclui malote(s) vinculado(s)
    const malVinc = malotes.filter(m => m.opId === id);
    const regVinc = regs.filter(r => r.opId === id);
    await Promise.all([
        deleteDoc(doc(db, 'operacoes', id)),
        ...malVinc.map(m => deleteDoc(doc(db, 'malotes', m.id))),
        ...regVinc.map(r => deleteDoc(doc(db, 'regularizacoes', r.id)))
    ]);
    const msg = ['Operação excluída'];
    if (malVinc.length > 0) msg.push('malote removido');
    if (regVinc.length > 0) msg.push('pendência removida');
    toast(msg.join(' · '), 'ok');
};
window.editMalFD = function () { const id = _detMalId; closeM('d-mal'); editMal(id); }
window.delMalFD = async function () { if (!confirm('Excluir?')) return; closeM('d-mal'); await deleteDoc(doc(db, 'malotes', _detMalId)); toast('Excluído.'); }
window.editRegFD = function () { const id = _detRegId; closeM('d-reg'); editReg(id); }
window.delRegFD = async function () { if (!confirm('Excluir?')) return; closeM('d-reg'); await deleteDoc(doc(db, 'regularizacoes', _detRegId)); toast('Excluída.'); }

// ═══ EXCEL PASTE ═══
const XPC = {
    op: ['ordem', 'pa', 'produto', 'tipoAssinatura', 'cpf', 'clienteNum', 'nome', 'contrato', 'finalidade', 'dataOp', 'dataVenc', 'resolucao', 'regularizacao', 'situacao'],
    mal: ['pa', 'produto', 'tipoAssinatura', 'cpf', 'clienteNum', 'nome', 'contrato', 'finalidade', 'dataOp', 'dataVenc', 'situacao', 'statusMalote', 'docFaltando'],
    reg: ['pa', 'produto', 'cpf', 'nome', 'contrato', 'pendencia', 'resolucao', 'status']
};
const XPF = {
    op: { pa: 'op-pa', produto: 'op-pr', tipoAssinatura: 'op-tas', cpf: 'op-cpf', clienteNum: 'op-cln', nome: 'op-nm', contrato: 'op-ct', finalidade: 'op-fin', dataOp: 'op-dop', dataVenc: 'op-dvc', resolucao: 'op-res', regularizacao: 'op-reg', situacao: 'op-sit' },
    mal: { pa: 'mal-pa', produto: 'mal-pr', tipoAssinatura: 'mal-tas', cpf: 'mal-cpf', clienteNum: 'mal-cln', nome: 'mal-nm', contrato: 'mal-ct', finalidade: 'mal-fin', dataOp: 'mal-dop', dataVenc: 'mal-dvc', situacao: 'mal-sit', statusMalote: 'mal-st', docFaltando: 'mal-doc' },
    reg: { pa: 'reg-pa', produto: 'reg-pr', cpf: 'reg-cpf', nome: 'reg-nm', contrato: 'reg-ct', pendencia: 'reg-pend', resolucao: 'reg-res', status: 'reg-st' }
};
const XPL = { pa: 'PA', produto: 'Produto', tipoAssinatura: 'Tipo', cpf: 'CPF/CNPJ', clienteNum: 'Nº Cliente', nome: 'Nome', contrato: 'Contrato', finalidade: 'Finalidade', dataOp: 'Data Op.', dataVenc: 'Data Venc.', resolucao: 'Resolução', regularizacao: 'Regularização', situacao: 'Situação', statusMalote: 'Status Malote', docFaltando: 'Doc. Faltando', pendencia: 'Pendência', status: 'Status', ordem: 'Ordem' };

window.xpToggle = function (id) { const el = g(id); el.classList.toggle('hidden'); if (!el.classList.contains('hidden')) { const ta = el.querySelector('textarea'); if (ta) { ta.value = ''; ta.focus(); } const pr = el.querySelector('.xp-prev'); if (pr) { pr.classList.remove('on'); pr.innerHTML = ''; } } }
window.xpPreview = function (txt, prevId, type) {
    const prev = g(prevId); if (!txt.trim()) { prev.classList.remove('on'); prev.innerHTML = ''; return; }
    const cols = txt.trim().split('\n')[0].split('\t').map(c => c.trim()); const map = XPC[type];
    let html = '<div style="font-size:11px;font-weight:700;color:var(--muted);margin-bottom:6px">Dados detectados:</div><div class="xp-cols">';
    cols.forEach((val, i) => { if (!val) return; const key = map[i]; const lbl = key ? XPL[key] || key : 'Col' + (i + 1); html += `<div class="xp-col"><span>${lbl}</span><strong>${esc(val.slice(0, 18))}</strong></div>`; });
    prev.innerHTML = html + '</div>'; prev.classList.add('on');
}
window.xpApply = function (taId, type) {
    const raw = g(taId).value.trim(); if (!raw) { toast('Cole dados do Excel primeiro.', 'err'); return; }
    const cols = raw.split('\n')[0].split('\t').map(c => c.trim()); const map = XPC[type]; const fields = XPF[type]; let filled = 0;
    cols.forEach((val, i) => {
        if (!val || !map[i]) return; const key = map[i]; const fid = fields[key]; if (!fid) return; const el = g(fid); if (!el) return;
        let fv = val; if (['dataOp', 'dataVenc', 'resolucao'].includes(key)) fv = toISO(val); if (key === 'produto') fv = val.toUpperCase();
        el.value = fv; el.classList.remove('flashed'); void el.offsetWidth; el.classList.add('flashed'); filled++;
    });
    toast('✅ ' + filled + ' campos preenchidos!', 'ok');
    setTimeout(() => { const p = g(taId).closest('.xp'); if (p) p.classList.add('hidden'); }, 700);
}
window.xpClear = function (taId, prevId) { g(taId).value = ''; const p = g(prevId); p.classList.remove('on'); p.innerHTML = ''; }

// ═══ CRUD — OPERAÇÕES ═══
window.newOp = function () { if (!isAdmin()) return; g('op-eid').value = ''; g('m-op-t').textContent = 'Nova Operação CCB'; setVals({ 'op-pa': '', 'op-pr': '', 'op-tas': '', 'op-cpf': '', 'op-cln': '', 'op-nm': '', 'op-ct': '', 'op-fin': '', 'op-dop': '', 'op-dvc': '', 'op-res': '', 'op-reg': '', 'op-sit': 'Aberto' }); g('op-err').classList.add('hidden'); g('btn-xp-op').classList.remove('hidden'); openM('m-op'); }
window.editOp = function (id) { if (!isAdmin()) return; const o = ops.find(x => x.id === id); if (!o) return; g('op-eid').value = id; g('m-op-t').textContent = 'Editar Operação'; setVals({ 'op-pa': o.pa, 'op-pr': o.produto, 'op-tas': o.tipoAssinatura, 'op-cpf': o.cpf, 'op-cln': o.clienteNum, 'op-nm': o.nome, 'op-ct': o.contrato, 'op-fin': o.finalidade, 'op-dop': o.dataOp, 'op-dvc': o.dataVenc, 'op-res': o.resolucao, 'op-reg': o.regularizacao, 'op-sit': o.situacao }); g('btn-xp-op').classList.add('hidden'); g('xp-op').classList.add('hidden'); openM('m-op'); }
window.saveOp = async function () {
    if (!isAdmin()) { toast('Sem permissão.', 'err'); return; }
    const eid = g('op-eid').value, err = g('op-err'); err.classList.add('hidden');
    const d = {
        pa: v('op-pa'), produto: v('op-pr'), tipoAssinatura: v('op-tas'),
        cpf: v('op-cpf'), clienteNum: v('op-cln'), nome: v('op-nm'),
        contrato: v('op-ct'), finalidade: v('op-fin'),
        dataOp: v('op-dop'), dataVenc: v('op-dvc'), resolucao: v('op-res'),
        regularizacao: v('op-reg'), situacao: v('op-sit')
    };
    if (!d.pa || !d.nome || !d.contrato) { showErr(err, 'PA, Nome e Contrato são obrigatórios.'); return; }

    // ── Validação: contrato único no sistema (não pode existir em outro PA) ──
    const ctDup = ops.find(o =>
        o.contrato.trim().toLowerCase() === d.contrato.trim().toLowerCase() &&
        o.id !== eid
    );
    if (ctDup) {
        showErr(err, `Contrato "${d.contrato}" já existe (PA ${paLabel(ctDup.pa)}). Um contrato não pode ser cadastrado em dois PAs diferentes.`);
        return;
    }

    // Bloco de dados compartilhado para malote e pendência
    const dadosMalote = {
        pa: d.pa, produto: d.produto, tipoAssinatura: d.tipoAssinatura,
        cpf: d.cpf, clienteNum: d.clienteNum, nome: d.nome,
        contrato: d.contrato, finalidade: d.finalidade,
        dataOp: d.dataOp, dataVenc: d.dataVenc, situacao: d.situacao,
        docFaltando: d.regularizacao && d.regularizacao.trim() ? d.regularizacao : ''
    };
    const dadosReg = {
        pa: d.pa, produto: d.produto, cpf: d.cpf, nome: d.nome,
        contrato: d.contrato,
        pendencia: 'Regularização da operação: ' + d.regularizacao
    };

    try {
        if (eid) {
            // ══ EDITAR ══ — atualiza operação e sincroniza TUDO pelo opId
            await updateDoc(doc(db, 'operacoes', eid), d);

            // Malote(s) vinculados
            const malVinc = malotes.filter(m => m.opId === eid);
            if (malVinc.length > 0) {
                await Promise.all(malVinc.map(m => updateDoc(doc(db, 'malotes', m.id), dadosMalote)));
            }

            // Pendência(s) vinculadas
            const regVinc = regs.filter(r => r.opId === eid);
            if (regVinc.length > 0) {
                await Promise.all(regVinc.map(r => updateDoc(doc(db, 'regularizacoes', r.id), {
                    ...dadosReg,
                    status: d.regularizacao && d.regularizacao.trim() ? r.status : 'Regularizada'
                })));
            } else if (d.regularizacao && d.regularizacao.trim()) {
                // Regularização adicionada agora — cria pendência
                await addDoc(collection(db, 'regularizacoes'), {
                    ...dadosReg, opId: eid, resolucao: '', status: 'Aberta',
                    criadoPor: CU.uid, createdAt: serverTimestamp()
                });
                await addN(d.pa, 'Nova pendência — ' + paLabel(d.pa) + ' · ' + d.contrato + ': ' + d.regularizacao);
            }

            const extras = [];
            if (malVinc.length > 0) extras.push(malVinc.length + ' malote(s) sincronizado(s)');
            if (regVinc.length > 0) extras.push('pendência atualizada');
            toast('Operação atualizada' + (extras.length ? '\n' + extras.join(' · ') : ''), 'ok');

        } else {
            // ══ NOVA ══ — cria operação, malote e pendência todos vinculados pelo opId
            const maxOrdem = ops.length ? Math.max(...ops.map(o => parseInt(o.ordem) || 0)) + 1 : 1;
            const opRef = await addDoc(collection(db, 'operacoes'), {
                ordem: maxOrdem, ...d, criadoPor: CU.uid, createdAt: serverTimestamp()
            });
            const opId = opRef.id;

            // Malote criado automaticamente vinculado pelo opId
            await addDoc(collection(db, 'malotes'), {
                ...dadosMalote, opId, statusMalote: 'Pendente',
                criadoPor: CU.uid, createdAt: serverTimestamp()
            });

            // Pendência criada automaticamente se tiver regularização
            if (d.regularizacao && d.regularizacao.trim()) {
                await addDoc(collection(db, 'regularizacoes'), {
                    ...dadosReg, opId, resolucao: '', status: 'Aberta',
                    criadoPor: CU.uid, createdAt: serverTimestamp()
                });
                await addN(d.pa, 'Nova pendência — ' + paLabel(d.pa) + ' · ' + d.contrato + ': ' + d.regularizacao);
                toast('Operação cadastrada! Malote e pendência criados automaticamente.', 'ok');
            } else {
                toast('Operação cadastrada! Malote criado automaticamente.', 'ok');
            }
        }
        closeM('m-op');
    } catch (e) { showErr(err, 'Erro ao salvar: ' + e.message); }
};

// ═══ CRUD — MALOTE ═══
window.newMal = function () { if (!isAdmin()) return; g('mal-eid').value = ''; g('m-mal-t').textContent = 'Novo Malote CCB'; setVals({ 'mal-pa': '', 'mal-pr': '', 'mal-tas': '', 'mal-cpf': '', 'mal-cln': '', 'mal-nm': '', 'mal-ct': '', 'mal-fin': '', 'mal-dop': '', 'mal-dvc': '', 'mal-sit': 'Aberto', 'mal-st': 'Pendente', 'mal-doc': '' }); g('mal-err').classList.add('hidden'); openM('m-mal'); }
window.editMal = function (id) { if (!isAdmin()) return; const m = malotes.find(x => x.id === id); if (!m) return; g('mal-eid').value = id; g('m-mal-t').textContent = 'Editar Malote'; setVals({ 'mal-pa': m.pa, 'mal-pr': m.produto, 'mal-tas': m.tipoAssinatura, 'mal-cpf': m.cpf, 'mal-cln': m.clienteNum, 'mal-nm': m.nome, 'mal-ct': m.contrato, 'mal-fin': m.finalidade, 'mal-dop': m.dataOp, 'mal-dvc': m.dataVenc, 'mal-sit': m.situacao, 'mal-st': m.statusMalote, 'mal-doc': m.docFaltando }); openM('m-mal'); }
window.saveMal = async function () {
    if (!isAdmin()) { toast('Sem permissão.', 'err'); return; }
    const eid = g('mal-eid').value, err = g('mal-err'), doc2 = v('mal-doc'); err.classList.add('hidden');
    const d = { pa: v('mal-pa'), produto: v('mal-pr'), tipoAssinatura: v('mal-tas'), cpf: v('mal-cpf'), clienteNum: v('mal-cln'), nome: v('mal-nm'), contrato: v('mal-ct'), finalidade: v('mal-fin'), dataOp: v('mal-dop'), dataVenc: v('mal-dvc'), situacao: v('mal-sit'), statusMalote: v('mal-st'), docFaltando: doc2 };
    if (!d.pa || !d.nome || !d.contrato) { showErr(err, 'PA, Nome e Contrato são obrigatórios.'); return; }
    try {
        if (eid) { await updateDoc(doc(db, 'malotes', eid), d); toast('Malote atualizado!', 'ok'); }
        else {
            await addDoc(collection(db, 'malotes'), { ...d, criadoPor: CU.uid, createdAt: serverTimestamp() });
            if (doc2.trim()) {
                await addDoc(collection(db, 'regularizacoes'), { pa: d.pa, produto: d.produto, cpf: d.cpf, nome: d.nome, contrato: d.contrato, pendencia: 'Doc. faltando no malote: ' + doc2, resolucao: '', status: 'Aberta', createdAt: serverTimestamp() });
                await addN(d.pa, 'Pendência automática — ' + paLabel(d.pa) + ' · ' + d.contrato + ': ' + doc2);
                toast('Malote salvo e pendência criada!', 'ok');
            } else toast('Malote cadastrado!', 'ok');
        }
        closeM('m-mal');
    } catch (e) { showErr(err, 'Erro: ' + e.message); }
};

// ═══ CRUD — REGULARIZAÇÃO ═══
window.newReg = function () { if (!isAdmin()) return; g('reg-eid').value = ''; g('m-reg-t').textContent = 'Nova Pendência'; setVals({ 'reg-pa': '', 'reg-pr': '', 'reg-cpf': '', 'reg-nm': '', 'reg-ct': '', 'reg-pend': '', 'reg-res': '', 'reg-st': 'Aberta' }); g('reg-err').classList.add('hidden'); openM('m-reg'); }
window.editReg = function (id) { if (!isAdmin()) return; const r = regs.find(x => x.id === id); if (!r) return; g('reg-eid').value = id; g('m-reg-t').textContent = 'Editar Pendência'; setVals({ 'reg-pa': r.pa, 'reg-pr': r.produto, 'reg-cpf': r.cpf, 'reg-nm': r.nome, 'reg-ct': r.contrato, 'reg-pend': r.pendencia, 'reg-res': r.resolucao, 'reg-st': r.status }); openM('m-reg'); }
window.saveReg = async function () {
    if (!isAdmin()) { toast('Sem permissão.', 'err'); return; }
    const eid = g('reg-eid').value, err = g('reg-err'); err.classList.add('hidden');
    const d = { pa: v('reg-pa'), produto: v('reg-pr'), cpf: v('reg-cpf'), nome: v('reg-nm'), contrato: v('reg-ct'), pendencia: v('reg-pend'), resolucao: v('reg-res'), status: v('reg-st') };
    if (!d.pa || !d.nome || !d.contrato || !d.pendencia) { showErr(err, 'PA, Nome, Contrato e Pendência são obrigatórios.'); return; }
    try {
        if (eid) { await updateDoc(doc(db, 'regularizacoes', eid), d); toast('Pendência atualizada!', 'ok'); }
        else { await addDoc(collection(db, 'regularizacoes'), { ...d, criadoPor: CU.uid, createdAt: serverTimestamp() }); toast('Pendência cadastrada!', 'ok'); }
        closeM('m-reg');
    } catch (e) { showErr(err, 'Erro: ' + e.message); }
};

// ═══ RENDER TABLES ═══
function fd(data, sId, filters) {
    const q = (g(sId)?.value || '').toLowerCase();
    return data.filter(row => {
        if (q && !JSON.stringify(row).toLowerCase().includes(q)) return false;
        for (const [fi, k] of filters) { const vl = (g(fi)?.value || ''); if (vl && row[k] !== vl) return false; }
        return true;
    });
}

window.renderOps = function () {
    const d = fd(ops, 'o-search', [['o-fsi', 'situacao'], ['o-fpa', 'pa'], ['o-fpr', 'produto']]);
    g('tb-ops').innerHTML = d.length ? d.map(o => `
    <tr class="tr-click" onclick="showOpDetail('${o.id}')">
      <td><span style="color:var(--muted);font-size:12px">#${esc(String(o.ordem || ''))}</span></td>
      <td>${paBadge(o.pa)}</td>
      <td><strong>${esc(o.nome)}</strong><div style="font-size:11px;color:var(--muted)">${esc(o.cpf || '')}</div></td>
      <td><span style="font-family:monospace;font-size:12px">${esc(o.contrato || '')}</span></td>
      <td>${pB(o.produto)}</td>
      <td>${fmtD(o.dataVenc) || '—'}</td>
      <td>${o.regularizacao ? '<span class="b b-or">⚠ Pendente</span>' : '<span class="b b-gr">OK</span>'}</td>
      <td>${sB(o.situacao)}</td>
    </tr>`).join('') : '<tr><td colspan="8" class="empty">Nenhuma operação encontrada</td></tr>';
};
window.renderMalotes = function () {
    const d = fd(malotes, 'm-search', [['m-fst', 'statusMalote'], ['m-fpa', 'pa']]);
    g('tb-mal').innerHTML = d.length ? d.map(m => `
    <tr class="tr-click" onclick="showMalDetail('${m.id}')">
      <td>${paBadge(m.pa)}</td>
      <td><strong>${esc(m.nome)}</strong><div style="font-size:11px;color:var(--muted)">${esc(m.cpf || '')}</div></td>
      <td><span style="font-family:monospace;font-size:12px">${esc(m.contrato || '')}</span></td>
      <td>${pB(m.produto)}</td>
      <td>${fmtD(m.dataOp) || '—'}</td>
      <td>${fmtD(m.dataVenc) || '—'}</td>
      <td>${mB(m.statusMalote)}</td>
    </tr>`).join('') : '<tr><td colspan="7" class="empty">Nenhum malote encontrado</td></tr>';
};
window.renderRegs = function () {
    const d = fd(regs, 'r-search', [['r-fst', 'status'], ['r-fpa', 'pa']]);
    g('tb-reg').innerHTML = d.length ? d.map(r => `
    <tr class="tr-click" onclick="showRegDetail('${r.id}')">
      <td>${paBadge(r.pa)}</td>
      <td><strong>${esc(r.nome)}</strong><div style="font-size:11px;color:var(--muted)">${esc(r.cpf || '')}</div></td>
      <td><span style="font-family:monospace;font-size:12px">${esc(r.contrato || '')}</span></td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.pendencia)}</td>
      <td>${fmtD(r.createdAt) || '—'}</td>
      <td>${rB(r.status)}</td>
    </tr>`).join('') : '<tr><td colspan="6" class="empty">Nenhuma pendência encontrada</td></tr>';
};

async function renderUsers() {
    if (!isAdmin()) return;
    const tb = g('tb-users');
    tb.innerHTML = '<tr><td colspan="6" class="empty">Carregando...</td></tr>';
    try {
        // Sem orderBy para evitar índice composto
        const snap = await getDocs(collection(db, 'users'));
        const allUsers = snap.docs.map(d => ({ uid: d.id, ...d.data() }))
            .sort((a, b) => {
                const at = a.createdAt?.toDate?.().getTime() || 0;
                const bt = b.createdAt?.toDate?.().getTime() || 0;
                return bt - at;
            });
        if (!allUsers.length) { tb.innerHTML = '<tr><td colspan="6" class="empty">Nenhum usuário cadastrado</td></tr>'; return; }
        const statusLabel = {
            active: '<span class="b b-gr">Ativo</span>',
            pending: '<span class="b b-yl">Aguardando</span>',
            reproved: '<span class="b b-rd">Reprovado</span>'
        };
        tb.innerHTML = allUsers.map(u => `<tr class="${u.status === 'pending' ? 'u-pending' : u.status === 'reproved' ? 'u-reproved' : ''}">
      <td><strong>${esc(u.name || '—')}</strong></td>
      <td style="font-size:12px">${esc(u.email || '—')}</td>
      <td>${paBadge(u.pa || '—')}</td>
      <td>${u.role === 'admin' ? '<span class="b b-pu">Admin</span>' : '<span class="b b-gy">Usuário PA</span>'}</td>
      <td>${statusLabel[u.status] || '<span class="b b-gy">—</span>'}</td>
      <td style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
        ${u.status === 'pending'
                ? `<button class="btn-apv" onclick="approveUser('${u.uid}')">✔ Aprovar</button><button class="btn-rep" onclick="reproveUser('${u.uid}')">✘ Reprovar</button>`
                : ''}
        ${u.status === 'active' && u.role !== 'admin'
                ? `<button class="btn-icon" onclick="promoteUser('${u.uid}')">↑ Tornar Admin</button>`
                : ''}
        ${u.status === 'reproved' ? '<span style="font-size:11px;color:var(--muted)">Acesso negado</span>' : ''}
      </td>
    </tr>`).join('');
    } catch (e) {
        console.error('renderUsers', e);
        tb.innerHTML = `<tr><td colspan="6" class="empty" style="color:var(--red)">Erro ao carregar usuários: ${e.message}</td></tr>`;
    }
}

// ═══ IMPORT ═══
window.doImport = async function () {
    if (!isAdmin()) { toast('Sem permissão.', 'err'); return; }
    const raw = g('imp-txt').value.trim(); if (!raw) { toast('Cole os dados primeiro.', 'err'); return; }
    const lines = raw.split('\n').filter(l => l.trim()); const start = isNaN(lines[0].split('\t')[0]) ? 1 : 0;
    let cnt = 0; const batch = [];
    for (let i = start; i < lines.length; i++) {
        const c = lines[i].split('\t'); if (c.length < 3) continue;
        const d = { ordem: parseInt(c[0]?.trim()) || 0, pa: c[1]?.trim() || '', produto: (c[2]?.trim() || '').toUpperCase(), tipoAssinatura: c[3]?.trim() || '', cpf: c[4]?.trim() || '', clienteNum: c[5]?.trim() || '', nome: c[6]?.trim() || '', contrato: c[7]?.trim() || '', finalidade: c[8]?.trim() || '', dataOp: toISO(c[9]?.trim() || ''), dataVenc: toISO(c[10]?.trim() || ''), resolucao: toISO(c[11]?.trim() || ''), regularizacao: c[12]?.trim() || '', situacao: c[13]?.trim() || 'Aberto' };
        if (!d.nome && !d.contrato) continue;
        batch.push(d); cnt++;
    }
    try {
        await Promise.all(batch.map(d => addDoc(collection(db, 'operacoes'), { ...d, criadoPor: CU.uid, createdAt: serverTimestamp() })));
        closeM('m-import'); g('imp-txt').value = '';
        toast(cnt + ' operações importadas!', 'ok');
    } catch (e) { toast('Erro na importação: ' + e.message, 'err'); }
};

// ═══ EXPORT ═══
function buildTSV(headers, rows) {
    const esc2 = s => '"' + (String(s || '').replace(/"/g, '""').replace(/\n/g, ' ')) + '"';
    return [headers.map(esc2).join('\t'), ...rows.map(r => r.map(esc2).join('\t'))].join('\n');
}
function downloadTSV(content, filename) {
    const b = new Blob(['\uFEFF' + content], { type: 'text/tab-separated-values;charset=utf-8;' });
    const a = document.createElement('a'); a.href = URL.createObjectURL(b);
    a.download = filename + '_' + new Date().toISOString().slice(0, 10) + '.xls';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
    toast('Download iniciado!', 'ok');
}
window.exportCSV = function (tid, fn) {
    if (tid === 't-ops') {
        const rows = ops.map(o => [o.ordem || '', paLabel(o.pa), o.nome, o.cpf, o.clienteNum, o.contrato, o.produto, o.tipoAssinatura, o.finalidade, fmtD(o.dataOp), fmtD(o.dataVenc), fmtD(o.resolucao), o.regularizacao, o.situacao]);
        downloadTSV(buildTSV(['#', 'PA', 'Nome', 'CPF/CNPJ', 'Nº Cliente', 'Contrato', 'Produto', 'Tipo Assin.', 'Finalidade', 'Data Op.', 'Data Venc.', 'Resolução', 'Regularização', 'Situação'], rows), fn);
    } else if (tid === 't-mal') {
        const rows = malotes.map(m => [paLabel(m.pa), m.nome, m.cpf, m.contrato, m.produto, m.tipoAssinatura, fmtD(m.dataOp), fmtD(m.dataVenc), m.situacao, m.statusMalote, m.docFaltando]);
        downloadTSV(buildTSV(['PA', 'Nome', 'CPF/CNPJ', 'Contrato', 'Produto', 'Tipo Assin.', 'Data Op.', 'Data Venc.', 'Situação', 'Status Malote', 'Doc. Faltando'], rows), fn);
    } else if (tid === 't-reg') {
        const rows = regs.map(r => [paLabel(r.pa), r.nome, r.cpf, r.contrato, r.produto, r.pendencia, fmtD(r.createdAt), fmtD(r.resolucao), r.status]);
        downloadTSV(buildTSV(['PA', 'Nome', 'CPF/CNPJ', 'Contrato', 'Produto', 'Pendência', 'Data Abertura', 'Data Resolução', 'Status'], rows), fn);
    }
};

// ═══ FILTER HELPERS ═══
function ppf(selId, data) {
    const sel = g(selId); if (!sel) return; const c = sel.value;
    const pas = [...new Set(data.map(d => d.pa).filter(Boolean))].sort();
    sel.innerHTML = '<option value="">Todos PAs</option>' + pas.map(p => `<option value="${esc(p)}"${c === p ? ' selected' : ''}>${paLabel(p)}</option>`).join('');
}

// ═══ BADGES ═══
function sB(s) { const m = { 'Aberto': 'b-bl', 'Encerrado': 'b-gr', 'Cancelado': 'b-gy', 'Repactuada': 'b-yl', 'Transf. Prejuízo': 'b-or', 'Prejuízo Quitado': 'b-rd' }; return `<span class="b ${m[s] || 'b-gy'}">${esc(s || '')}</span>` }
function mB(s) { const m = { 'Pendente': 'b-or', 'Recebido': 'b-bl', 'Regularizado': 'b-gr' }; return `<span class="b ${m[s] || 'b-gy'}">${esc(s || '')}</span>` }
function rB(s) { const m = { 'Aberta': 'b-rd', 'Em Análise': 'b-yl', 'Regularizada': 'b-gr' }; return `<span class="b ${m[s] || 'b-gy'}">${esc(s || '')}</span>` }
function pB(p) { const m = { 'RURAL': 'b-gr', 'COMERCIAL': 'b-bl', 'LIMITE': 'b-pu' }; return p ? `<span class="b ${m[p] || 'b-gy'}">${esc(p)}</span>` : ''; }

// Expose for inline onclick
window.showOpDetail = window.showOpDetail;
window.showMalDetail = window.showMalDetail;
window.showRegDetail = window.showRegDetail;
window.renderOps = window.renderOps;
window.renderMalotes = window.renderMalotes;
window.renderRegs = window.renderRegs;
window.renderUsers = renderUsers;