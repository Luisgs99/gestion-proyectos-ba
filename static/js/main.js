// ── Modal ──────────────────────────────────────────
function openModal(title, bodyHtml) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-body').innerHTML = bodyHtml;
  document.getElementById('modal-overlay').classList.add('open');
}
function closeModal() {
  document.getElementById('modal-overlay').classList.remove('open');
}

// ── Tabs ───────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.tab;
      const container = btn.closest('.tabs-container');
      if (!container) return;
      container.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      container.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      const content = container.querySelector(`[data-tab-content="${target}"]`);
      if (content) content.classList.add('active');
    });
  });

  // Auto-dismiss alerts after 5s
  setTimeout(() => {
    document.querySelectorAll('.alert').forEach(a => a.remove());
  }, 6000);
});

// ── Toggle hito form ───────────────────────────────
function toggleHitoForm(hitoId) {
  const form = document.getElementById(`hito-form-${hitoId}`);
  if (form) form.classList.toggle('open');
}

// ── Confirm delete ─────────────────────────────────
function confirmDelete(msg, formId) {
  if (confirm(msg || '¿Estás seguro de que querés eliminar este elemento?')) {
    document.getElementById(formId).submit();
  }
}

// ── Format money ───────────────────────────────────
function formatMoney(n) {
  if (!n) return '$0';
  if (n >= 1000000) return '$' + (n/1000000).toFixed(1) + 'M';
  if (n >= 1000) return '$' + (n/1000).toFixed(0) + 'K';
  return '$' + n.toLocaleString('es-AR');
}

// ── Chart.js defaults ──────────────────────────────
if (typeof Chart !== 'undefined') {
  Chart.defaults.font.family = "'IBM Plex Sans', sans-serif";
  Chart.defaults.font.size = 12;
  Chart.defaults.color = '#7A8FA6';
  Chart.defaults.plugins.legend.display = true;
  Chart.defaults.plugins.legend.labels.boxWidth = 12;
  Chart.defaults.plugins.legend.labels.padding = 16;
  Chart.defaults.plugins.tooltip.backgroundColor = '#0A1628';
  Chart.defaults.plugins.tooltip.titleFont = { family: "'Sora', sans-serif", size: 13, weight: '600' };
  Chart.defaults.plugins.tooltip.bodyFont = { family: "'IBM Plex Sans', sans-serif", size: 12 };
  Chart.defaults.plugins.tooltip.padding = 10;
  Chart.defaults.plugins.tooltip.cornerRadius = 8;
}

// ── Upload zone drag and drop ──────────────────────
document.addEventListener('DOMContentLoaded', () => {
  const zones = document.querySelectorAll('.upload-zone');
  zones.forEach(zone => {
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
      e.preventDefault();
      zone.classList.remove('drag-over');
      const fileInput = zone.querySelector('input[type="file"]');
      if (fileInput && e.dataTransfer.files[0]) {
        fileInput.files = e.dataTransfer.files;
        const name = e.dataTransfer.files[0].name;
        const label = zone.querySelector('.upload-filename');
        if (label) label.textContent = name;
      }
    });
  });
});

// ── ANR formatter ──────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.anr-value').forEach(el => {
    const n = parseFloat(el.dataset.value || 0);
    el.textContent = formatMoney(n);
  });
});
