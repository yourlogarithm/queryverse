import { useState } from 'react';
import { Input } from 'antd';

const App = () => {
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState([]);
  const { Search } = Input;

  const onSearch = async (value: string) => {
    setLoading(true);
    const response = await fetch('http://localhost:8000/api/v1/search', {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify({ query: value })
    });
    const data = await response.json();
    setResults(data);
  }

  return (<div>
    <Search
      placeholder="Something you're looking for"
      enterButton="Search"
      size="large"
      loading={loading}
      onSearch={onSearch}
    />
    {results}
  </div>);
}


export default App;
