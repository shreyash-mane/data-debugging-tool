import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import UploadPage from './components/UploadPage';
import PipelineBuilder from './components/PipelineBuilder';
import DebuggerPage from './components/DebuggerPage';
// @ts-ignore — DatasetFinderPage is a .jsx file without TS declarations
import DatasetFinderPage from './pages/DatasetFinderPage';

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
        {/* Dataset Finder — renders with its own full-page UI */}
        <Route path="/dataset-finder" element={<DatasetFinderPage />} />
        {/* OAuth callback route for Dataset Finder social login */}
        <Route path="/auth/callback" element={<DatasetFinderPage />} />
      </Routes>
    </BrowserRouter>
  );
}
