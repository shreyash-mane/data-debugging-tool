import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import UploadPage from './components/UploadPage';
import PipelineBuilder from './components/PipelineBuilder';
import DebuggerPage from './components/DebuggerPage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Navigate to="/upload" replace />} />
          <Route path="/upload" element={<UploadPage />} />
          <Route path="/pipeline/:pipelineId" element={<PipelineBuilder />} />
          <Route path="/debug/:pipelineId/:runId" element={<DebuggerPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
