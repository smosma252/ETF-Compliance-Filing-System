import './App.css'
import UploadPage from "./pages/Upload"
import FundsPage from './pages/Funds'
import ExceptionsPage from './pages/Exceptions'
import AuditPage from './pages/Audit'
import HoldingsPage from './pages/Holdings'
import Home from "./pages/Home"
import { Route, Switch } from "wouter"

function App() {

  return (
    <>
    <Switch>
      <Route path="/" component={Home}></Route>
      <Route path="/Funds" component={FundsPage}>Funds</Route>
      <Route path="/Holdings" component={HoldingsPage}>Holdings</Route>
      <Route path="/Exceptions" component={ExceptionsPage}>Exceptions</Route>
      <Route path="/Upload" component={UploadPage}></Route>
      <Route path="/Audit" component={AuditPage}>Audit</Route>
    </Switch>
     
    </>
  )
}

export default App
