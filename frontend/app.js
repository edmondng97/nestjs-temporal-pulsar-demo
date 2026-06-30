// Render the flow nodes and wire click → detail panel. Pure DOM, no fetch.
(function () {
  const flow = document.getElementById('flow');
  window.NODES.forEach((node, i) => {
    if (i > 0) {
      const a = document.createElement('span');
      a.className = 'arrow';
      a.textContent = '→';
      flow.appendChild(a);
    }
    const el = document.createElement('div');
    el.className = 'node';
    el.innerHTML = '<h4>' + node.title + '</h4>';
    el.addEventListener('click', () => select(node, el));
    flow.appendChild(el);
  });

  const states = document.getElementById('states');
  window.STATES.forEach((s) => {
    const el = document.createElement('span');
    el.className = 'state';
    el.textContent = s;
    states.appendChild(el);
  });

  function select(node, el) {
    document.querySelectorAll('.node').forEach((n) => n.classList.remove('active'));
    el.classList.add('active');
    document.getElementById('detail-title').textContent = node.title;
    document.getElementById('detail-what').textContent = node.what;
    document.getElementById('detail-why').textContent = node.why;
    document.getElementById('detail-code').textContent = node.code;
  }

  if (window.NODES.length) select(window.NODES[0], flow.querySelector('.node'));
})();
